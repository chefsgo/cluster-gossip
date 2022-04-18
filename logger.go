package cluster_gossip

type (
	Logger struct {
	}
)

func (logger *Logger) Write(p []byte) (n int, err error) {
	// fmt.Println("logger", string(p))
	return 0, nil
}
