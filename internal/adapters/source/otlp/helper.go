package otlp

import "syscall"

func isPortAvailableUnix(port int) bool {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return false
	}
	defer syscall.Close(fd)

	addr := syscall.SockaddrInet4{
		Port: port,
		Addr: [4]byte{127, 0, 0, 1}, // 127.0.0.1
	}

	err = syscall.Bind(fd, &addr)
	if err != nil {
		return false
	}
	return true
}
