package cmd

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/distribworks/dkron/v3/dkron"
	dkplugin "github.com/distribworks/dkron/v3/plugin"
	"github.com/hashicorp/go-plugin"
	"github.com/kardianos/osext"
	"github.com/sirupsen/logrus"
)

type Plugins struct {
	Processors map[string]dkplugin.Processor
	Executors  map[string]dkplugin.Executor
	LogLevel   string
	NodeName   string
}

// Discover plugins located on disk
//
// We look in the following places for plugins:
//
// 1. Dkron configuration path
// 2. Path where Dkron is installed
//
// Whichever file is discoverd LAST wins.
func (p *Plugins) DiscoverPlugins() error {
	p.Processors = make(map[string]dkplugin.Processor)
	p.Executors = make(map[string]dkplugin.Executor)

	// Look in /etc/dkron/plugins
	// 匹配目录下的文件列表
	processors, err := plugin.Discover("dkron-processor-*", filepath.Join("/etc", "dkron", "plugins"))
	if err != nil {
		return err
	}

	// Look in /etc/dkron/plugins
	executors, err := plugin.Discover("dkron-executor-*", filepath.Join("/etc", "dkron", "plugins"))
	if err != nil {
		return err
	}

	// Next, look in the same directory as the Dkron executable, usually
	// /usr/local/bin. If found, this replaces what we found in the config path.
	exePath, err := osext.Executable()
	if err != nil {
		logrus.WithError(err).Error("Error loading exe directory")
	} else {
		// 从当前 agent 执行目录查找，同目录的可执行文件
		// 默认执行目录为 /opt/local/dkron/
		// 若我们需要添加自定义插件, 将编译后的二进制文件放入同目录即可
		p, err := plugin.Discover("dkron-processor-*", filepath.Dir(exePath))
		if err != nil {
			return err
		}
		processors = append(processors, p...)
		e, err := plugin.Discover("dkron-executor-*", filepath.Dir(exePath))
		if err != nil {
			return err
		}
		executors = append(executors, e...)
	}


	// pluginFactory
	// 创建运行时

	for _, file := range processors {

		// 文件名按 "-" 分隔，取最后一个元素
		// 问题点: 插件名不支持带 "-"
		pluginName, ok := getPluginName(file)
		if !ok {
			continue
		}

		raw, err := p.pluginFactory(file, dkplugin.ProcessorPluginName)
		if err != nil {
			return err
		}
		p.Processors[pluginName] = raw.(dkplugin.Processor)
	}

	for _, file := range executors {

		pluginName, ok := getPluginName(file)
		if !ok {
			continue
		}

		raw, err := p.pluginFactory(file, dkplugin.ExecutorPluginName)
		if err != nil {
			return err
		}
		p.Executors[pluginName] = raw.(dkplugin.Executor)
	}

	return nil
}

func getPluginName(file string) (string, bool) {
	// Look for foo-bar-baz. The plugin name is "baz"
	base := path.Base(file)
	parts := strings.SplitN(base, "-", 3)
	if len(parts) != 3 {
		return "", false
	}

	// This cleans off the .exe for windows plugins
	name := strings.TrimSuffix(parts[2], ".exe")
	return name, true
}

// 创建 Plugin Client
func (p *Plugins) pluginFactory(path string, pluginType string) (interface{}, error) {
	// Build the plugin client configuration and init the plugin
	var config plugin.ClientConfig
	// 可执行文件
	config.Cmd = exec.Command(path)
	// handshake 配置是 client 与 server 约定的 TOKEN,
	//	不通过环境变量包含同样的 TOKEN 则无法启动 plugin 程序.
	//	具体查看 go-plugin 项目 https://github.com/mayocream/go-plugin/blob/044aadd925bf9f027cb301b2af9bc6b60775dd22/server.go#L248
	config.HandshakeConfig = dkplugin.Handshake
	// go-plugin 包会在 NewClient 的时候储存 Client 的指针,
	//	能够调用 cleanClients 统一 Kill 所有的 Client
	config.Managed = true
	// 定义一个二进制所能包含的不同插件
	config.Plugins = dkplugin.PluginMap
	config.SyncStdout = os.Stdout
	config.SyncStderr = os.Stderr
	config.Logger = &dkron.HCLogAdapter{Logger: dkron.InitLogger(p.LogLevel, p.NodeName), LoggerName: "plugins"}

	switch pluginType {
	case dkplugin.ProcessorPluginName:
		// processor 使用 golang net/rpc 进行通信
		config.AllowedProtocols = []plugin.Protocol{plugin.ProtocolNetRPC}
	case dkplugin.ExecutorPluginName:
		// executor 使用 gprc 进行通信
		config.AllowedProtocols = []plugin.Protocol{plugin.ProtocolGRPC}
	}

	// 初始化客户端
	client := plugin.NewClient(&config)

	// Request the RPC client so we can get the provider
	// so we can build the actual RPC-implemented provider.
	// go-plugin Client() 会用 exec.Start 启动 plugin server,
	//	创建 rpc client, 这个库将连接复用, 以及 rpc/grpc service 挂载
	//	等细节屏蔽了, 开发者只要创建业务逻辑 Interface 并实现 plugin.Plguin 的
	//	Serve/Client 方法就能够进行 rpc 通信
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	// 调用指定的 service
	raw, err := rpcClient.Dispense(pluginType)
	if err != nil {
		return nil, err
	}

	return raw, nil
}
