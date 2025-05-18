package lsm

type ImmutableLayer struct {
	data map[string]string
}

func NewImmutableLayer(data map[string]string) *ImmutableLayer {
	return &ImmutableLayer{
		data: data,
	}
}

func (il *ImmutableLayer) Get(key string) (string, bool) {
	value, exists := il.data[key]
	return value, exists
}
