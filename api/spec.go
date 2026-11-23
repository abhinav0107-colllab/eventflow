// Package apispec embeds the OpenAPI 3.0 specification of the EventFlow API.
package apispec

import _ "embed"

// OpenAPI is the YAML OpenAPI document served at /openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPI []byte
