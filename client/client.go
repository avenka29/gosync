// Package client re-exports the zishang520 Socket.IO client used with GoSync.
package client

import upstream "github.com/zishang520/socket.io/clients/socket/v3"

type (
	Socket                  = upstream.Socket
	Manager                 = upstream.Manager
	Options                 = upstream.Options
	OptionsInterface        = upstream.OptionsInterface
	ManagerOptions          = upstream.ManagerOptions
	ManagerOptionsInterface = upstream.ManagerOptionsInterface
	SocketOptions           = upstream.SocketOptions
	SocketOptionsInterface  = upstream.SocketOptionsInterface
)

func DefaultOptions() *Options {
	return upstream.DefaultOptions()
}

func DefaultManagerOptions() *ManagerOptions {
	return upstream.DefaultManagerOptions()
}

func DefaultSocketOptions() *SocketOptions {
	return upstream.DefaultSocketOptions()
}

func Connect(uri string, options OptionsInterface) (*Socket, error) {
	return upstream.Connect(uri, options)
}

func NewManager(uri string, options ManagerOptionsInterface) *Manager {
	return upstream.NewManager(uri, options)
}
