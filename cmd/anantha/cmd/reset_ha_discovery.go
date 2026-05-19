package cmd

import (
	"fmt"
	"time"

	mqtt_paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cobra"
)

var resetHADiscoveryCmd = &cobra.Command{
	Use:   "reset-ha-discovery",
	Short: "Clear the retained Home Assistant MQTT discovery message",
	Long: `Publish empty retained payloads to clear Home Assistant MQTT discovery
messages so HA removes existing climate entity registrations. After this runs,
restart 'anantha serve' (or wait for the next thermostat reconnect) to republish
the discovery message; HA will register the entity fresh.

Two topics are cleared:
  - homeassistant/climate/<clientID>-zone<zone>/config  (current format)
  - homeassistant/climate/<clientID>/config              (legacy format, pre-multi-zone)

The --zone flag (default "1") selects which zone's topic to clear. The legacy
topic is always cleared regardless of zone, since older versions published without
a zone suffix.

This is also a migration step for users upgrading from a version of anantha that
did not include the 'device' block in the discovery payload or that used the
old single-topic format. After upgrading, HA may show an orphaned device card with
"no entities". Running this subcommand and then restarting 'anantha serve' resolves
the orphaned state.

Run this while 'anantha serve' is stopped, otherwise the running bridge may
republish discovery before HA has had time to remove the entity.

Example:
  anantha reset-ha-discovery \
    --ha-mqtt-addr 192.168.1.100:1883 \
    --ha-mqtt-username myuser \
    --ha-mqtt-password mypass \
    --client-id 4123X123456 \
    --zone 1`,
	RunE: runResetHADiscovery,
}

func init() {
	resetHADiscoveryCmd.Flags().String("ha-mqtt-addr", "", "Home Assistant MQTT host:port (required, e.g. 192.168.1.100:1883)")
	resetHADiscoveryCmd.Flags().String("ha-mqtt-username", "", "Home Assistant MQTT username")
	resetHADiscoveryCmd.Flags().String("ha-mqtt-password", "", "Home Assistant MQTT password")
	resetHADiscoveryCmd.Flags().String("client-id", "", "Thermostat Device Serial ID (required)")
	resetHADiscoveryCmd.Flags().String("zone", "1", "Thermostat zone number (default 1)")
	//nolint:errcheck
	resetHADiscoveryCmd.MarkFlagRequired("ha-mqtt-addr")
	//nolint:errcheck
	resetHADiscoveryCmd.MarkFlagRequired("client-id")
}

func runResetHADiscovery(cmd *cobra.Command, args []string) error {
	addr, _ := cmd.Flags().GetString("ha-mqtt-addr")
	username, _ := cmd.Flags().GetString("ha-mqtt-username")
	password, _ := cmd.Flags().GetString("ha-mqtt-password")
	clientID, _ := cmd.Flags().GetString("client-id")
	zone, _ := cmd.Flags().GetString("zone")

	currentTopic := fmt.Sprintf("homeassistant/climate/%s-zone%s/config", clientID, zone)
	legacyTopic := fmt.Sprintf("homeassistant/climate/%s/config", clientID)

	opts := mqtt_paho.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s", addr)).
		SetClientID(fmt.Sprintf("anantha-reset-%d", time.Now().Unix())).
		SetConnectTimeout(10 * time.Second)
	if username != "" {
		opts.SetUsername(username)
	}
	if password != "" {
		opts.SetPassword(password)
	}

	client := mqtt_paho.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker at %s: %w", addr, token.Error())
	}
	defer client.Disconnect(250)

	for _, topic := range []string{currentTopic, legacyTopic} {
		token := client.Publish(topic, 0, true, []byte(""))
		if !token.WaitTimeout(10 * time.Second) {
			return fmt.Errorf("timed out waiting for publish to %s", topic)
		}
		if err := token.Error(); err != nil {
			return fmt.Errorf("failed to publish to %s: %w", topic, err)
		}
	}

	fmt.Printf("Cleared retained HA discovery messages:\n  - %s\n  - %s\n", currentTopic, legacyTopic)
	fmt.Println("Restart 'anantha serve' or wait for the next thermostat reconnect to republish discovery with the device association.")
	return nil
}
