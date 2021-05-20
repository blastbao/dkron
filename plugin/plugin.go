package plugin

import (
	"github.com/hashicorp/go-plugin"
)

// See serve.go for serving plugins

// PluginMap should be used by clients for the map of plugins.
// Dkron 封装的 Plugin 结构, 规定可以有 processor 或 executor 两个插件
var PluginMap = map[string]plugin.Plugin{
	"processor": &ProcessorPlugin{},
	"executor":  &ExecutorPlugin{},
}
