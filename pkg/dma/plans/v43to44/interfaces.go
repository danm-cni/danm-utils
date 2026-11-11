package v43to44

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
)

const (
	OldInterfacesKey = OldGroup + "/interfaces"
	NewInterfacesKey = NewGroup + "/interfaces"
	CopiedMarkerKey  = "dma.danm.io/interfaces-copied"
	CopiedByShim     = "shim"
	SourcePodsKey    = "dma.danm.io/source-pods"
	dynamicIp        = "dynamic"
	tenantNetworkKey = "tenantNetwork"
	clusterNetKey    = "clusterNetwork"
	ipKey            = "ip"
	ip6Key           = "ip6"
)

type SourcePod struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type objectField struct {
	key   string
	value json.RawMessage
}

func NewInterfaces(old, namespace, name string, rips []*danmtypes.ReservedIP) (string, []string) {
	entries := []json.RawMessage{}
	if err := json.Unmarshal([]byte(old), &entries); err != nil {
		return old, nil
	}
	decoded := make([][]objectField, len(entries))
	for index, entry := range entries {
		fields, err := decodeObject(entry)
		if err != nil {
			return old, nil
		}
		decoded[index] = fields
	}
	changed := false
	notes := []string{}
	for _, fields := range decoded {
		candidates := ripCandidates(fields, namespace, rips)
		if len(candidates) == 0 {
			continue
		}
		for index := range fields {
			if fields[index].key != ipKey && fields[index].key != ip6Key {
				continue
			}
			value := ""
			if err := json.Unmarshal(fields[index].value, &value); err != nil {
				continue
			}
			replacement, note := ripReference(value, fields[index].key == ip6Key, namespace, name, candidates)
			if note != "" {
				notes = append(notes, note)
			}
			if replacement == "" {
				continue
			}
			encoded, err := json.Marshal(replacement)
			if err != nil {
				continue
			}
			fields[index].value = encoded
			changed = true
		}
	}
	if !changed {
		return old, notes
	}
	var buffer bytes.Buffer
	buffer.WriteByte('[')
	for index, fields := range decoded {
		if index > 0 {
			buffer.WriteByte(',')
		}
		encodeObject(&buffer, fields)
	}
	buffer.WriteByte(']')
	return buffer.String(), notes
}

func decodeObject(raw json.RawMessage) ([]objectField, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("not a JSON object")
	}
	fields := []objectField{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, isString := token.(string)
		if !isString {
			return nil, fmt.Errorf("not a JSON object")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields = append(fields, objectField{key: key, value: value})
	}
	return fields, nil
}

func encodeObject(buffer *bytes.Buffer, fields []objectField) {
	buffer.WriteByte('{')
	for index, field := range fields {
		if index > 0 {
			buffer.WriteByte(',')
		}
		key, _ := json.Marshal(field.key)
		buffer.Write(key)
		buffer.WriteByte(':')
		buffer.Write(field.value)
	}
	buffer.WriteByte('}')
}

func fieldString(fields []objectField, key string) string {
	for _, field := range fields {
		if field.key != key {
			continue
		}
		value := ""
		if err := json.Unmarshal(field.value, &value); err != nil {
			return ""
		}
		return value
	}
	return ""
}

func ripCandidates(fields []objectField, namespace string, rips []*danmtypes.ReservedIP) []*danmtypes.ReservedIP {
	networkName, networkType := "", ""
	if name := fieldString(fields, tenantNetworkKey); name != "" {
		networkName, networkType = name, kindTenantNetwork.Kind
	} else if name := fieldString(fields, clusterNetKey); name != "" {
		networkName, networkType = name, kindClusterNetwork.Kind
	} else {
		return nil
	}
	candidates := []*danmtypes.ReservedIP{}
	for _, rip := range rips {
		if rip.Namespace == namespace && rip.Spec.Network.Name == networkName && rip.Spec.Network.Type == networkType {
			candidates = append(candidates, rip)
		}
	}
	return candidates
}

func ripReference(value string, ipv6 bool, namespace, name string, candidates []*danmtypes.ReservedIP) (string, string) {
	requested := ""
	if value != dynamicIp {
		address, _, _ := strings.Cut(value, "/")
		parsed := net.ParseIP(address)
		if parsed == nil || (parsed.To4() == nil) != ipv6 {
			return "", ""
		}
		requested = parsed.String()
	} else if name == "" {
		return "", ""
	}
	matches := []string{}
	for _, rip := range candidates {
		address := ripAddress(rip, ipv6)
		if address == "" {
			continue
		}
		if (requested != "" && address == requested) || (requested == "" && isSourcePod(rip, namespace, name)) {
			matches = append(matches, rip.Name)
		}
	}
	switch len(matches) {
	case 0:
		return "", ""
	case 1:
		return matches[0], ""
	}
	return "", fmt.Sprintf("%q of Pod %s/%s matches ReservedIPs %s, left unchanged", value, namespace, name, strings.Join(matches, ", "))
}

func ripAddress(rip *danmtypes.ReservedIP, ipv6 bool) string {
	for _, ip := range rip.Spec.Ips {
		parsed := net.ParseIP(ip.Address)
		if parsed != nil && (parsed.To4() == nil) == ipv6 {
			return parsed.String()
		}
	}
	return ""
}

func isSourcePod(rip *danmtypes.ReservedIP, namespace, name string) bool {
	raw := rip.Annotations[SourcePodsKey]
	if raw == "" {
		return false
	}
	pods := []SourcePod{}
	if err := json.Unmarshal([]byte(raw), &pods); err != nil {
		return false
	}
	for _, pod := range pods {
		if pod.Namespace == namespace && pod.Name == name {
			return true
		}
	}
	return false
}
