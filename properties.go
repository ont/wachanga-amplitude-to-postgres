package main

type Properties map[string]interface{}

func (p Properties) GetString(key string) string {
	if ivalue, found := p[key]; found {
		if value, ok := ivalue.(string); ok {
			return value
		}
	}
	return ""
}

func (p Properties) GetBool(key string) bool {
	if ivalue, found := p[key]; found {
		if value, ok := ivalue.(bool); ok {
			return value
		}
	}
	return false
}
