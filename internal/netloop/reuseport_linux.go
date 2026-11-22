//go:build linux && !mips && !mipsle && !mips64 && !mips64le

package netloop

// soReusePort is SO_REUSEPORT from the Linux kernel UAPI headers. The syscall
// package does not expose it on linux/amd64 (and several other architectures),
// so the documented constant is defined here. Its value is the same on every
// Go-supported non-MIPS Linux architecture.
const soReusePort = 0xf
