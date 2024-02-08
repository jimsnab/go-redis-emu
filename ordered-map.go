package redisemu

type (
	orderedMap struct {
		m     map[string]any
		order []string
	}

	orderedRespMap struct {
		m     map[respValue]respValue
		order []respValue
	}
)

func newOrderedMap() *orderedMap {
	return &orderedMap{
		m:     map[string]any{},
		order: []string{},
	}
}

func (om *orderedMap) set(k string, v any) {
	_, exists := om.m[k]
	if !exists {
		om.order = append(om.order, k)
	}
	om.m[k] = v
}

func (om *orderedMap) get(k string) (v any, exists bool) {
	v, exists = om.m[k]
	return
}

func (om *orderedMap) mustGet(k string) (v any) {
	return om.m[k]
}

func (om *orderedMap) toNative() map[string]any {
	m := map[string]any{}
	for k, v := range om.m {
		switch t := v.(type) {
		case *orderedMap:
			m[k] = t.toNative()
		default:
			m[k] = t
		}
	}

	return m
}

func newRespMap() respMap {
	return respMap{
		orderedRespMap: orderedRespMap{
			m:     map[respValue]respValue{},
			order: []respValue{},
		},
	}
}

func newRespMapSized(size int) respMap {
	return respMap{
		orderedRespMap: orderedRespMap{
			m:     make(map[respValue]respValue, size),
			order: make([]respValue, 0, size),
		},
	}
}
func (orm *orderedRespMap) set(k respValue, v respValue) {
	_, exists := orm.m[k]
	if !exists {
		orm.order = append(orm.order, k)
	}
	orm.m[k] = v
}

func (orm *orderedRespMap) get(k respValue) (v respValue, exists bool) {
	v, exists = orm.m[k]
	return
}

func (orm *orderedRespMap) mustGet(k respValue) (v respValue) {
	v = orm.m[k]
	return
}
