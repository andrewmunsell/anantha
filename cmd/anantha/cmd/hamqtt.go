package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	carrier "github.com/anupcshan/anantha/pb"
	mqtt_paho "github.com/eclipse/paho.mqtt.golang"
)

const maxZones = 8

type HAMQTT struct {
	addr         string
	topicPrefix  string
	username     string
	password     string
	clientID     string
	loadedValues *LoadedValues

	sendCommand func([]*carrier.ConfigSetting)
	mqttClient  mqtt_paho.Client
}

func NewHAMQTT(addr string, topicPrefix string, username string, password string, clientID string, loadedValues *LoadedValues, sendCommand func([]*carrier.ConfigSetting)) *HAMQTT {
	return &HAMQTT{
		addr:         addr,
		topicPrefix:  topicPrefix,
		username:     username,
		password:     password,
		clientID:     clientID,
		loadedValues: loadedValues,
		sendCommand:  sendCommand,
	}
}

func invertMap[K, V comparable](m map[K]V) map[V]K {
	i := make(map[V]K)
	for k, v := range m {
		i[v] = k
	}

	return i
}

var (
	// Translate Carrier mode to Home Assistant HVAC mode
	carrierModeToHAMode = map[string]string{
		"auto":    "auto",
		"off":     "off",
		"cool":    "cool",
		"heat":    "heat",
		"fanonly": "fan_only",
	}

	HAModeToCarrierMode = invertMap(carrierModeToHAMode)

	// Translate Carrier fan mode to Home Assistant fan mode
	carrierFanModeToHA = map[string]string{
		"off":  "auto",
		"low":  "low",
		"med":  "medium",
		"high": "high",
	}

	HAFanModeToCarrier = invertMap(carrierFanModeToHA)
)

// Need to return one of:
// off, heating, cooling, drying, idle, fan
func computeCurrentAction(opmode string, opstat string) string {
	switch opmode {
	case "heating", "off":
		return opmode
	case "cooling":
		if opstat != "dehumidify" {
			return opmode
		} else {
			return "drying"
		}
	}

	return "off"
}

func (h *HAMQTT) publish(topicSuffix string, value string) error {
	token := h.mqttClient.Publish(
		fmt.Sprintf("%s/%s", h.topicPrefix, topicSuffix),
		0, true,
		value,
	)

	token.Wait()
	return token.Error()
}

func (h *HAMQTT) publishRaw(topic string, value string) error {
	token := h.mqttClient.Publish(
		topic,
		0, true,
		value,
	)

	token.Wait()
	return token.Error()
}

func (h *HAMQTT) subscribe(topicSuffix string, handler func(_ mqtt_paho.Client, msg mqtt_paho.Message)) {
	token := h.mqttClient.Subscribe(
		fmt.Sprintf("%s/%s", h.topicPrefix, topicSuffix),
		0,
		handler,
	)

	token.Wait()
}

func (h *HAMQTT) installSubscriptions() {
	h.subscribe("mode/set",
		func(_ mqtt_paho.Client, msg mqtt_paho.Message) {
			log.Printf("About to set mode to %s", msg.Payload())
			h.sendCommand([]*carrier.ConfigSetting{
				{
					Name:       "system/mode",
					ConfigType: carrier.ConfigType_CT_STRING,
					Value: &carrier.ConfigSetting_MaybeStrValue{
						MaybeStrValue: []byte(HAModeToCarrierMode[string(msg.Payload())]),
					},
				},
			})
		},
	)
}

func (h *HAMQTT) subscribeZone(zone string) {
	h.subscribe(fmt.Sprintf("zone/%s/fanmode/set", zone),
		func(_ mqtt_paho.Client, msg mqtt_paho.Message) {
			activityVal := h.loadedValues.Get(fmt.Sprintf("%s/currentActivity", zone))
			if activityVal.value == nil {
				return
			}
			currentActivity := string(activityVal.value.GetMaybeStrValue())
			log.Printf("About to set fan mode for %s to %s", currentActivity, msg.Payload())
			h.sendCommand([]*carrier.ConfigSetting{
				{
					Name:       fmt.Sprintf("zones/%s/activities/%s/fan", zone, currentActivity),
					ConfigType: carrier.ConfigType_CT_STRING,
					Value: &carrier.ConfigSetting_MaybeStrValue{
						MaybeStrValue: []byte(HAFanModeToCarrier[string(msg.Payload())]),
					},
				},
			})
		},
	)

	h.subscribe(fmt.Sprintf("zone/%s/preset_mode/set", zone),
		func(_ mqtt_paho.Client, msg mqtt_paho.Message) {
			switch string(msg.Payload()) {
			case "none":
				// Reset to schedule
				log.Printf("About to reset preset mode for zone %s", zone)
				h.sendCommand([]*carrier.ConfigSetting{
					{
						Name:       fmt.Sprintf("zones/%s/hold/hold", zone),
						ConfigType: carrier.ConfigType_CT_BOOL,
						Value: &carrier.ConfigSetting_BoolValue{
							BoolValue: false,
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/holdActivity", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value:      &carrier.ConfigSetting_MaybeStrValue{},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/otmr", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value:      &carrier.ConfigSetting_MaybeStrValue{},
					},
				})
			default:
				log.Printf("About to set preset mode for zone %s to %s", zone, msg.Payload())
				h.sendCommand([]*carrier.ConfigSetting{
					{
						Name:       fmt.Sprintf("zones/%s/hold/hold", zone),
						ConfigType: carrier.ConfigType_CT_BOOL,
						Value: &carrier.ConfigSetting_BoolValue{
							BoolValue: true,
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/holdActivity", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value: &carrier.ConfigSetting_MaybeStrValue{
							MaybeStrValue: msg.Payload(),
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/otmr", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value:      &carrier.ConfigSetting_MaybeStrValue{},
					},
				})
			}
		},
	)

	h.subscribe(fmt.Sprintf("zone/%s/temp_high/set", zone),
		func(_ mqtt_paho.Client, msg mqtt_paho.Message) {
			log.Printf("About to set high temp for zone %s to %s", zone, msg.Payload())
			clsp, err := strconv.ParseFloat(string(msg.Payload()), 32)
			if err != nil {
				log.Printf("Unable to parse cool setpoint %s: %s", msg.Payload(), err)
			}
			var cfgSettings []*carrier.ConfigSetting
			if string(h.loadedValues.Get(fmt.Sprintf("%s/currentActivity", zone)).value.GetMaybeStrValue()) != "manual" {
				cfgSettings = append(cfgSettings, []*carrier.ConfigSetting{
					{
						Name:       fmt.Sprintf("zones/%s/hold/hold", zone),
						ConfigType: carrier.ConfigType_CT_BOOL,
						Value: &carrier.ConfigSetting_BoolValue{
							BoolValue: true,
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/holdActivity", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value: &carrier.ConfigSetting_MaybeStrValue{
							MaybeStrValue: []byte("manual"),
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/otmr", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value:      &carrier.ConfigSetting_MaybeStrValue{},
					},
					{
						Name:       fmt.Sprintf("zones/%s/activities/manual/htsp", zone),
						ConfigType: carrier.ConfigType_CT_FLOAT,
						Value: &carrier.ConfigSetting_FloatValue{
							FloatValue: h.loadedValues.Get(fmt.Sprintf("%s/htsp", zone)).value.GetFloatValue(),
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/activities/manual/fan", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value: &carrier.ConfigSetting_MaybeStrValue{
							MaybeStrValue: h.loadedValues.Get(fmt.Sprintf("%s/fan", zone)).value.GetMaybeStrValue(),
						},
					},
				}...)
			}

			cfgSettings = append(cfgSettings, []*carrier.ConfigSetting{
				{
					Name:       fmt.Sprintf("zones/%s/activities/manual/clsp", zone),
					ConfigType: carrier.ConfigType_CT_FLOAT,
					Value: &carrier.ConfigSetting_FloatValue{
						FloatValue: float32(clsp),
					},
				},
			}...,
			)
			h.sendCommand(cfgSettings)
		},
	)

	h.subscribe(fmt.Sprintf("zone/%s/temp_low/set", zone),
		func(_ mqtt_paho.Client, msg mqtt_paho.Message) {
			log.Printf("About to set low temp for zone %s to %s", zone, msg.Payload())
			htsp, err := strconv.ParseFloat(string(msg.Payload()), 32)
			if err != nil {
				log.Printf("Unable to parse heat setpoint %s: %s", msg.Payload(), err)
			}
			var cfgSettings []*carrier.ConfigSetting
			// If current activity is already manual, don't send the same values down again.
			// NOTE: There is a problem here interacting with Home Assistant. It sends both
			// "temp_low/set" and "temp_high/set" when changing temperature, in that order.
			// So the value of "htsp" set by "temp_low/set" gets clobbered by "temp_high/set"
			// handler immediately after, because we haven't yet gotten a response from the
			// thermostat to update "<zone>/htsp".
			if string(h.loadedValues.Get(fmt.Sprintf("%s/currentActivity", zone)).value.GetMaybeStrValue()) != "manual" {
				cfgSettings = append(cfgSettings, []*carrier.ConfigSetting{
					{
						Name:       fmt.Sprintf("zones/%s/hold/hold", zone),
						ConfigType: carrier.ConfigType_CT_BOOL,
						Value: &carrier.ConfigSetting_BoolValue{
							BoolValue: true,
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/holdActivity", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value: &carrier.ConfigSetting_MaybeStrValue{
							MaybeStrValue: []byte("manual"),
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/hold/otmr", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value:      &carrier.ConfigSetting_MaybeStrValue{},
					},
					{
						Name:       fmt.Sprintf("zones/%s/activities/manual/clsp", zone),
						ConfigType: carrier.ConfigType_CT_FLOAT,
						Value: &carrier.ConfigSetting_FloatValue{
							FloatValue: h.loadedValues.Get(fmt.Sprintf("%s/clsp", zone)).value.GetFloatValue(),
						},
					},
					{
						Name:       fmt.Sprintf("zones/%s/activities/manual/fan", zone),
						ConfigType: carrier.ConfigType_CT_STRING,
						Value: &carrier.ConfigSetting_MaybeStrValue{
							MaybeStrValue: h.loadedValues.Get(fmt.Sprintf("%s/fan", zone)).value.GetMaybeStrValue(),
						},
					},
				}...)
			}

			cfgSettings = append(cfgSettings, []*carrier.ConfigSetting{
				{
					Name:       fmt.Sprintf("zones/%s/activities/manual/htsp", zone),
					ConfigType: carrier.ConfigType_CT_FLOAT,
					Value: &carrier.ConfigSetting_FloatValue{
						FloatValue: float32(htsp),
					},
				},
			}...,
			)
			h.sendCommand(cfgSettings)
		},
	)
}

func (h *HAMQTT) publishDiscovery(zone string) {
	serialVal := h.loadedValues.Get("profile/serial")
	if serialVal.value == nil {
		return
	}
	serial := string(serialVal.value.GetMaybeStrValue())

	deviceField := func(key string) string {
		v := h.loadedValues.Get(key)
		if v.value == nil {
			return ""
		}
		return string(v.value.GetMaybeStrValue())
	}

	type haDevice struct {
		Identifiers  []string `json:"identifiers"`
		Name         string   `json:"name"`
		Manufacturer string   `json:"manufacturer,omitempty"`
		Model        string   `json:"model,omitempty"`
		SWVersion    string   `json:"sw_version,omitempty"`
	}

	uniqueID := h.clientID + "-zone" + zone
	configTopic := fmt.Sprintf("homeassistant/climate/%s-zone%s/config", serial, zone)

	// Zone 1 uses the legacy topic and unique_id for backward compatibility
	// with pre-multi-zone deployments.
	if zone == "1" {
		uniqueID = h.clientID
		configTopic = fmt.Sprintf("homeassistant/climate/%s/config", serial)
	}

	discoveryMsg := struct {
		Name                        string   `json:"name"`
		ActionTopic                 string   `json:"action_topic"`
		CurrentHumidityTopic        string   `json:"current_humidity_topic"`
		CurrentTemperatureTopic     string   `json:"current_temperature_topic"`
		FanModeCommandTopic         string   `json:"fan_mode_command_topic"`
		FanModeStateTopic           string   `json:"fan_mode_state_topic"`
		TemperatureLowCommandTopic  string   `json:"temperature_low_command_topic"`
		TemperatureLowStateTopic    string   `json:"temperature_low_state_topic"`
		TemperatureHighCommandTopic string   `json:"temperature_high_command_topic"`
		TemperatureHighStateTopic   string   `json:"temperature_high_state_topic"`
		PresetModeCommandTopic      string   `json:"preset_mode_command_topic"`
		PresetModeStateTopic        string   `json:"preset_mode_state_topic"`
		PresetModes                 []string `json:"preset_modes"`
		ModeCommandTopic            string   `json:"mode_command_topic"`
		ModeStateTopic              string   `json:"mode_state_topic"`
		Modes                       []string `json:"modes"`
		UniqueID                    string   `json:"unique_id"`
		Device                      haDevice `json:"device"`
	}{
		Name:                        "carrier",
		ActionTopic:                 fmt.Sprintf("%s/action/current", h.topicPrefix),
		CurrentHumidityTopic:        fmt.Sprintf("%s/zone/%s/humidity/current", h.topicPrefix, zone),
		CurrentTemperatureTopic:     fmt.Sprintf("%s/zone/%s/temperature/current", h.topicPrefix, zone),
		FanModeCommandTopic:         fmt.Sprintf("%s/zone/%s/fanmode/set", h.topicPrefix, zone),
		FanModeStateTopic:           fmt.Sprintf("%s/zone/%s/fanmode/current", h.topicPrefix, zone),
		TemperatureLowCommandTopic:  fmt.Sprintf("%s/zone/%s/temp_low/set", h.topicPrefix, zone),
		TemperatureLowStateTopic:    fmt.Sprintf("%s/zone/%s/temp_low/current", h.topicPrefix, zone),
		TemperatureHighCommandTopic: fmt.Sprintf("%s/zone/%s/temp_high/set", h.topicPrefix, zone),
		TemperatureHighStateTopic:   fmt.Sprintf("%s/zone/%s/temp_high/current", h.topicPrefix, zone),
		PresetModeCommandTopic:      fmt.Sprintf("%s/zone/%s/preset_mode/set", h.topicPrefix, zone),
		PresetModeStateTopic:        fmt.Sprintf("%s/zone/%s/preset_mode/current", h.topicPrefix, zone),
		PresetModes:                 []string{"away", "home", "manual", "sleep", "wake", "vacation"},
		ModeCommandTopic:            fmt.Sprintf("%s/mode/set", h.topicPrefix),
		ModeStateTopic:              fmt.Sprintf("%s/mode/current", h.topicPrefix),
		Modes:                       []string{"auto", "off", "cool", "heat", "fan_only"},
		UniqueID:                    uniqueID,
		Device: haDevice{
			Identifiers:  []string{serial},
			Name:         "Carrier Infinity",
			Manufacturer: deviceField("profile/brand"),
			Model:        deviceField("profile/model"),
			SWVersion:    deviceField("profile/firmware"),
		},
	}
	discoveryMsgJSON, err := json.Marshal(discoveryMsg)
	if err != nil {
		log.Printf("Failed to encode discovery message: %s", err)
		return
	}

	if err := h.publishRaw(configTopic, string(discoveryMsgJSON)); err != nil {
		log.Printf("Error publishing discovery message: %s", err)
	}
}

func (h *HAMQTT) registerCallbacks(zone string) {
	h.loadedValues.OnChange1(fmt.Sprintf("%s/clsp", zone), func(clsp TimestampedValue) {
		// Causes climate card to show nothing if we send None here.

		// var value string
		// switch string(mode.value.GetMaybeStrValue()) {
		// case "cool", "auto":
		// 	value = fmt.Sprintf("%.1f", clsp.value.GetFloatValue())
		// default:
		// 	value = "None"
		// }

		// if err := h.publish(fmt.Sprintf("zone/%s/temp_high/current", zone), value); err != nil {
		// 	log.Printf("Error publishing: %s", err)
		// }

		if err := h.publish(fmt.Sprintf("zone/%s/temp_high/current", zone), fmt.Sprintf("%.1f", clsp.value.GetFloatValue())); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})

	h.loadedValues.OnChange1(fmt.Sprintf("%s/htsp", zone), func(htsp TimestampedValue) {
		// var value string
		// switch string(mode.value.GetMaybeStrValue()) {
		// case "heat", "auto":
		// 	value = fmt.Sprintf("%.1f", htsp.value.GetFloatValue())
		// default:
		// 	value = "None"
		// }

		// if err := h.publish(fmt.Sprintf("zone/%s/temp_low/current", zone), value); err != nil {
		// 	log.Printf("Error publishing: %s", err)
		// }

		if err := h.publish(fmt.Sprintf("zone/%s/temp_low/current", zone), fmt.Sprintf("%.1f", htsp.value.GetFloatValue())); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})

	h.loadedValues.OnChange1(fmt.Sprintf("%s/fan", zone), func(mode TimestampedValue) {
		if err := h.publish(
			fmt.Sprintf("zone/%s/fanmode/current", zone),
			carrierFanModeToHA[string(mode.value.GetMaybeStrValue())],
		); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})

	h.loadedValues.OnChange1(fmt.Sprintf("%s/currentActivity", zone), func(activity TimestampedValue) {
		if err := h.publish(
			fmt.Sprintf("zone/%s/preset_mode/current", zone),
			string(activity.value.GetMaybeStrValue()),
		); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})

	h.loadedValues.OnChange1(fmt.Sprintf("%s/rh", zone), func(rh TimestampedValue) {
		if err := h.publish(
			fmt.Sprintf("zone/%s/humidity/current", zone),
			fmt.Sprintf("%f", rh.value.GetFloatValue()),
		); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})

	h.loadedValues.OnChange1(fmt.Sprintf("%s/rt", zone), func(rt TimestampedValue) {
		if err := h.publish(
			fmt.Sprintf("zone/%s/temperature/current", zone),
			fmt.Sprintf("%.1f", rt.value.GetFloatValue()),
		); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})
}

func (h *HAMQTT) Run() {
	if h.addr == "" && h.topicPrefix == "" {
		log.Printf("Not initialiazing HA MQTT with addr=%s topicPrefix=%s", h.addr, h.topicPrefix)
		return
	}

	var clientOptions = mqtt_paho.NewClientOptions().
		AddBroker(h.addr).
		SetAutoReconnect(true).
		SetClientID(h.clientID).
		SetConnectionLostHandler(func(c mqtt_paho.Client, err error) {
			log.Printf("MQTT connection lost: %v", err)
		}).
		SetOnConnectHandler(func(c mqtt_paho.Client) {
			log.Printf("MQTT connection established to %s", h.addr)
			h.installSubscriptions()
			for zone := 1; zone <= maxZones; zone++ {
				h.subscribeZone(fmt.Sprintf("%d", zone))
			}
		})
	if h.username != "" && h.password != "" {
		clientOptions.SetUsername(h.username)
		clientOptions.SetPassword(h.password)
	}
	h.mqttClient = mqtt_paho.NewClient(clientOptions)

	log.Printf("Connecting to %s", h.addr)
	if token := h.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to MQTT: %s", token.Error())
	}
	log.Printf("Connected to %s", h.addr)

	h.loadedValues.OnChange1("profile/serial", func(tv TimestampedValue) {
		for zone := 1; zone <= maxZones; zone++ {
			zoneStr := fmt.Sprintf("%d", zone)
			if h.loadedValues.Get(fmt.Sprintf("%s/rt", zoneStr)).value != nil {
				h.publishDiscovery(zoneStr)
			}
		}
	})

	// Publish discovery when a new zone's room temperature data appears
	zoneRTRegex := regexp.MustCompile("^[1-8]/rt$")
	h.loadedValues.OnChangeRegex(zoneRTRegex, func(tv TimestampedValue) {
		zone := strings.SplitN(tv.value.Name, "/", 2)[0]
		h.publishDiscovery(zone)
	})

	h.loadedValues.OnChange2("opmode", "opstat", func(opmode, opstat TimestampedValue) {
		if err := h.publish(
			"action/current",
			computeCurrentAction(
				string(opmode.value.GetMaybeStrValue()),
				string(opstat.value.GetMaybeStrValue()),
			),
		); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})

	h.loadedValues.OnChange1("system/mode", func(mode TimestampedValue) {
		if err := h.publish(
			"mode/current",
			carrierModeToHAMode[string(mode.value.GetMaybeStrValue())],
		); err != nil {
			log.Printf("Error publishing: %s", err)
		}
	})

	for zone := 1; zone <= maxZones; zone++ {
		h.registerCallbacks(fmt.Sprintf("%d", zone))
	}
}
