//修改自etcetera
package etcdstruct

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	//"github.com/coreos/go-etcd/etcd"

	"etcdcli/etcd"
	etcdv2 "github.com/coreos/etcd/client"
	//"time"
)

var (
	// ErrInvalidConfig alert whenever you try to use something that is not a structure in the Save
	// function, or something that is not a pointer to a structure in the Load funciton
	ErrInvalidConfig = errors.New("etcetera: configuration must be a structure or a pointer to a structure")

	// ErrNotInitialized alert when you pass a structure to the Load function that has a map attribute
	// that is not initialized
	ErrNotInitialized = errors.New("etcetera: configuration has fields that are not initialized (map)")

	// ErrFieldNotMapped alert whenever you try to access a field that wasn't loaded in the client
	// structure. If we don't load the field before we cannot determinate the path or version
	ErrFieldNotMapped = errors.New("etcetera: trying to retrieve information of a field that wasn't previously loaded")

	// ErrFieldNotAddr is throw when a field that cannot be addressable is used in a place that we
	// need the pointer to identify the path related to the field
	ErrFieldNotAddr = errors.New("etcetera: field must be a pointer or an addressable value")
)

// https://github.com/coreos/etcd/blob/master/error/error.go
const (
	etcdErrorCodeKeyNotFound  etcdErrorCode = 100 // used in tests
	etcdErrorCodeNotFile      etcdErrorCode = 102 // used in tests
	etcdErrorCodeNodeExist    etcdErrorCode = 105
	etcdErrorCodeRaftInternal etcdErrorCode = 300 // used in tests
)

type etcdErrorCode int

// Client stores the etcd connection, the configuration instance that we are managing and some extra
// informations that are useful for controlling path versions and making the API simpler
type Client struct {
	etcdClient *etcd.Client
	namespace  string
	config     reflect.Value

	// info creates a correlation between a path to a info structure that stores some extra
	// information and make the API usage easier
	info map[string]info
}

type info struct {
	field   reflect.Value
	version uint64
}

// NewClient internally build a etcd client object (go-etcd library).
// The machines attribute defines the etcd cluster that this client will be connect to. Now the
// namespace defines a special root directory to build the configuration URIs, and is recommended
// when you want to use more than one configuration structure in the same etcd. And finally the
// config attribute is the configuration struct that you want to send or retrieve of etcd
func NewClient(machines []string, namespace string, config interface{}) (*Client, error) {
	configValue := reflect.ValueOf(config)

	if configValue.Kind() != reflect.Ptr ||
		configValue.Elem().Kind() != reflect.Struct {

		return nil, ErrInvalidConfig
	}

	etcdClient, err := etcd.NewClient(machines, "", 0)
	if (err != nil) {
		return nil, errors.New("etcd.NewClient")
	}
	c := &Client{
		etcdClient: etcdClient,
		namespace:  normalizeTag(namespace),
		config:     configValue,
		info:       make(map[string]info),
	}

	namespace = c.namespace
	if len(namespace) > 0 {
		namespace = "/" + namespace
	}

	c.preload(c.config, namespace)
	return c, nil
}
/*

// NewTLSClient internally build a etcd client object with TLS (go-etcd library). The machines
// attribute defines the etcd cluster that this client will be connect to. The cert, key, caCert
// attributes are the same from the go-etcd library to ensure the TLS connection. Now the namespace
// defines a special root directory to build the configuration URIs, and is recommended when you
// want to use more than one configuration structure in the same etcd. And finally the config
// attribute is the configuration struct that you want to send or retrieve of etcd
func NewTLSClient(machines []string, cert, key, caCert, namespace string, config interface{}) (*Client, error) {
	configValue := reflect.ValueOf(config)

	if configValue.Kind() != reflect.Ptr ||
		configValue.Elem().Kind() != reflect.Struct {

		return nil, ErrInvalidConfig
	}

	tlsClient, err := etcd.NewTLSClient(machines, cert, key, caCert)
	if err != nil {
		return nil, err
	}

	c := &Client{
		etcdClient: tlsClient,
		namespace:  normalizeTag(namespace),
		config:     configValue,
		info:       make(map[string]info),
	}

	namespace = c.namespace
	if len(namespace) > 0 {
		namespace = "/" + namespace
	}

	c.preload(c.config, namespace)
	return c, nil
}
*/
func (c *Client) GetClient() *etcd.Client {
	return c.etcdClient
}
func (c *Client) preload(field reflect.Value, prefix string) {
	field = field.Elem()

	switch field.Kind() {
	case reflect.Struct:
		for i := 0; i < field.NumField(); i++ {
			subfield := field.Field(i)
			subfieldType := field.Type().Field(i)

			path := normalizeTag(subfieldType.Tag.Get("etcd"))
			if len(path) == 0 {
				continue
			}
			path = prefix + "/" + path

			c.preload(subfield.Addr(), path)
		}
	}

	if len(prefix) == 0 {
		prefix = "/"
	}

	c.info[prefix] = info{
		field: field,
	}
}

// Save stores a structure in etcd.
// Only attributes with the tag 'etcd' are going to be saved. Supported types are 'struct', 'slice',
// 'map', 'string', 'int', 'int64' and 'bool'
func (c *Client) Save() error {
	namespace := c.namespace
	if len(namespace) > 0 {
		namespace = "/" + namespace
	}

	return c.saveField(c.config, namespace)
}

// SaveField saves a specific field from the configuration structure.
// Works in the same way of Save, but it can be used to save specific parts of the configuration,
// avoiding excessive requests to etcd cluster
func (c *Client) SaveField(field interface{}) error {
	path, _, err := c.getInfo(field)
	if err != nil {
		return err
	}

	return c.saveField(reflect.ValueOf(field), path)
}

func (c *Client) saveField(field reflect.Value, prefix string) error {
	if field.Kind() == reflect.Ptr {
		field = field.Elem()
	}

	switch field.Kind() {
	case reflect.Struct:
		for i := 0; i < field.NumField(); i++ {
			subfield := field.Field(i)
			subfieldType := field.Type().Field(i)

			path := normalizeTag(subfieldType.Tag.Get("etcd"))
			if len(path) == 0 {
				continue
			}
			path = prefix + "/" + path

			if err := c.saveField(subfield, path); err != nil {
				return err
			}
		}

	case reflect.Map:
		if err := c.etcdClient.MKDir(prefix, 0); err != nil && !etcd.IsEtcdNodeExist(err) {

			return err
		}

		for _, key := range field.MapKeys() {
			value := field.MapIndex(key)
			path := prefix + "/" + key.String()

			switch value.Kind() {
			case reflect.Struct:
				if err := c.saveField(value, path); err != nil {
					return err
				}

			case reflect.String:
				if err := c.etcdClient.Set(path, value.String(), 0, "", 0); err != nil {
					return err
				}
			}
		}

	case reflect.Slice:
		if err := c.etcdClient.MKDir(prefix, 0); err != nil && !etcd.IsEtcdNodeExist(err) {
			return err
		}

		for i := 0; i < field.Len(); i++ {
			item := field.Index(i)

			if item.Kind() == reflect.Struct {
				path := fmt.Sprintf("%s/%d", prefix, i)

				if err := c.etcdClient.MKDir(prefix, 0); err != nil && !etcd.IsEtcdNodeExist(err) {
					return err
				}

				if err := c.saveField(item, path); err != nil {
					return err
				}

			} else {
				/*
				if _, err := c.etcdClient.CreateInOrder(prefix, item.String(), 0); err != nil {
					return err
				}
				*/
				if err := c.etcdClient.MK(prefix, item.String(), 0, true); err != nil {
					return err
				}
			}
		}

	case reflect.String:
		value := field.Interface().(string)
		if err := c.etcdClient.Set(prefix, value, 0, "", 0); err != nil {
			return err
		}

	case reflect.Int:
		value := field.Interface().(int)
		if err := c.etcdClient.Set(prefix, strconv.FormatInt(int64(value), 10), 0, "", 0); err != nil {
			return err
		}

	case reflect.Int64:
		value := field.Interface().(int64)
		if err := c.etcdClient.Set(prefix, strconv.FormatInt(value, 10), 0, "", 0); err != nil {
			return err
		}

	case reflect.Bool:
		value := field.Interface().(bool)

		var valueStr string
		if value {
			valueStr = "true"
		} else {
			valueStr = "false"
		}

		if err := c.etcdClient.Set(prefix, valueStr, 0, "", 0); err != nil {
			return err
		}
	}

	c.info[prefix] = info{
		field: field,
	}

	return nil
}

/*
func alreadyExistsError(err error) bool {
	etcderr, ok := err.(*etcd.EtcdError)
	if !ok {
		return false
	}

	return etcderr.ErrorCode == int(etcdErrorCodeNodeExist)
}
*/
// Load retrieves the data from the etcd into the given structure.
// Only attributes with the tag 'etcd' will be filled. Supported types are 'struct', 'slice', 'map',
// 'string', 'int', 'int64' and 'bool'
func (c *Client) Load() error {
	namespace := c.namespace
	if len(namespace) > 0 {
		namespace = "/" + namespace
	}

	return c.load(c.config, namespace)
}

func (c *Client) load(config reflect.Value, prefix string) error {
	if config.Kind() != reflect.Ptr {
		return ErrInvalidConfig
	}
	config = config.Elem()

	for i := 0; i < config.NumField(); i++ {
		field := config.Field(i)
		fieldType := config.Type().Field(i)

		path := normalizeTag(fieldType.Tag.Get("etcd"))
		if len(path) == 0 {
			continue
		}
		path = prefix + "/" + path

		response, err := c.etcdClient.GetResonse(path, true, true)
		if err != nil {
			return err
		}

		if err := c.fillField(field, response.Node, path); err != nil {
			return err
		}
	}

	return nil
}

/*

// Watch keeps track of a specific field in etcd using a long polling strategy.
// When a change is detected the callback function will run. When you want to stop watching the
// field, just close the returning channel
//
// BUG(rafaeljusto): If the user sends a boolean false instead of closing the returning channel, we
// could have a strange behavior since there are two go routines listening on it (go-etcd and
// etcetera watch functions)
func (c *Client) Watch(field interface{}, callback func()) (chan<- bool, error) {
	path, _, err := c.getInfo(field)
	if err != nil {
		return nil, err
	}

	fieldValue := reflect.ValueOf(field)
	if fieldValue.Kind() == reflect.Ptr {
		fieldValue = fieldValue.Elem()
	}

	stop := make(chan bool)
	receiver := make(chan *etcd.Response)

	// We are always retrieving the last version (index) of the path
	go c.etcdClient.Watch(path, 0, true, receiver, stop)

	go func() {
		for {
			select {
			case response := <-receiver:
				if response != nil {
					// When watching a directory (slice, map or structure) the response will be from the node
					// that changed and not the entire directory. So we need to query the directory again with
					// recursion to load it correctly.
					response, err := c.etcdClient.Get(path, true, true)
					if err == nil {
						c.fillField(fieldValue, response.Node, path)
						callback()
					}
				}

			case <-stop:
				return
			}
		}
	}()

	return stop, nil
}
*/

func (c *Client) fillField(field reflect.Value, node *etcdv2.Node, prefix string) error {
	switch field.Kind() {
	case reflect.Struct:
		for i := 0; i < field.NumField(); i++ {
			subfield := field.Field(i)
			subfieldType := field.Type().Field(i)

			path := normalizeTag(subfieldType.Tag.Get("etcd"))
			if len(path) == 0 {
				continue
			}
			path = prefix + "/" + path

			for _, child := range node.Nodes {
				if path == child.Key {
					if err := c.fillField(subfield, child, path); err != nil {
						return err
					}
					break
				}
			}
		}

	case reflect.Map:
		field.Set(reflect.MakeMap(field.Type()))

		switch field.Type().Elem().Kind() {
		case reflect.Struct:
			for _, node := range node.Nodes {
				newStruct := reflect.New(field.Type().Elem()).Elem()
				if err := c.fillField(newStruct, node, node.Key); err != nil {
					return err
				}

				pathParts := strings.Split(node.Key, "/")

				field.SetMapIndex(
					reflect.ValueOf(pathParts[len(pathParts)-1]),
					newStruct,
				)
			}

		case reflect.String:
			for _, node := range node.Nodes {
				pathParts := strings.Split(node.Key, "/")

				field.SetMapIndex(
					reflect.ValueOf(pathParts[len(pathParts)-1]),
					reflect.ValueOf(node.Value),
				)
			}
		}

	case reflect.Slice:
		field.Set(reflect.MakeSlice(field.Type(), 0, len(node.Nodes)))

		switch field.Type().Elem().Kind() {
		case reflect.Struct:
			for i, item := range node.Nodes {
				newStruct := reflect.New(field.Type().Elem()).Elem()

			SubitemLoop:
				for _, subitem := range item.Nodes {
					for j := 0; j < newStruct.NumField(); j++ {
						subfield := newStruct.Field(j)
						subfieldType := newStruct.Type().Field(j)

						path := normalizeTag(subfieldType.Tag.Get("etcd"))
						if len(path) == 0 {
							continue
						}
						path = fmt.Sprintf("%s/%d/%s", prefix, i, path)

						if path == subitem.Key {
							if err := c.fillField(subfield, subitem, path); err != nil {
								return err
							}
							continue SubitemLoop
						}
					}
				}
				field.Set(reflect.Append(field, newStruct))
			}

		case reflect.String:
			for _, node := range node.Nodes {
				field.Set(reflect.Append(field, reflect.ValueOf(node.Value)))
			}

		case reflect.Int:
			for _, node := range node.Nodes {
				value, err := strconv.ParseInt(node.Value, 10, 64)
				if err != nil {
					return err
				}

				field.Set(reflect.Append(field, reflect.ValueOf(int(value))))
			}

		case reflect.Int64:
			for _, node := range node.Nodes {
				value, err := strconv.ParseInt(node.Value, 10, 64)
				if err != nil {
					return err
				}

				field.Set(reflect.Append(field, reflect.ValueOf(value)))
			}

		case reflect.Bool:
			for _, node := range node.Nodes {
				if node.Value == "true" {
					field.Set(reflect.Append(field, reflect.ValueOf(true)))
				} else if node.Value == "false" {
					field.Set(reflect.Append(field, reflect.ValueOf(false)))
				}
			}
		}

	case reflect.String:
		field.SetString(node.Value)

	case reflect.Int, reflect.Int64:
		value, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return err
		}

		field.SetInt(value)

	case reflect.Bool:
		if node.Value == "true" {
			field.SetBool(true)
		} else if node.Value == "false" {
			field.SetBool(false)
		}
	}

	c.info[node.Key] = info{
		field:   field,
		version: node.ModifiedIndex,
	}

	return nil
}

// Version returns the current version of a field retrieved from etcd.
// It does not query etcd for the latest version. When the field was not retrieved from etcd yet,
// the version 0 is returned
func (c *Client) Version(field interface{}) (uint64, error) {
	_, info, err := c.getInfo(field)
	if err != nil {
		return 0, err
	}
	return info.version, nil
}

func (c *Client) getInfo(field interface{}) (path string, info info, err error) {
	fieldValue := reflect.ValueOf(field)
	if fieldValue.Kind() == reflect.Ptr {
		fieldValue = fieldValue.Elem()

	} else if !fieldValue.CanAddr() {
		err = ErrFieldNotAddr
		return
	}

	found := false
	for path, info = range c.info {
		// Match the pointer, type and name to avoid problems for struct and first field that have the
		// same memory address
		if info.field.Addr().Pointer() == fieldValue.Addr().Pointer() &&
			info.field.Type().Name() == fieldValue.Type().Name() &&
			info.field.Kind() == fieldValue.Kind() {

			found = true
			break
		}
	}

	if !found {
		err = ErrFieldNotMapped
	}

	return
}

// normalizeTag removes the slash from the beggining or end of the tag name and replace the other
// slashs with hyphens. The idea is to limit the hierarchy to the configuration structure
func normalizeTag(tag string) string {
	return tag
	for strings.HasPrefix(tag, "/") {
		tag = strings.TrimPrefix(tag, "/")
	}

	for strings.HasSuffix(tag, "/") {
		tag = strings.TrimSuffix(tag, "/")
	}

	return strings.Replace(tag, "/", "-", -1)
}
