package mongonet

import (
	"fmt"
	"runtime"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

// ---

type ProxyRetryError struct {
	MsgToRetry     Message
	PreviousResult SimpleBSON
	RetryOnRs      string
	RetryCount     int
}

func (e *ProxyRetryError) Error() string {
	return fmt.Sprintf("ProxyRetryError - going to retry on %s, %v times remaining", e.RetryOnRs, e.RetryCount)
}

func NewProxyRetryError(msgToRetry Message, previousRes SimpleBSON, retryOnRs string) *ProxyRetryError {
	return &ProxyRetryError{
		msgToRetry,
		previousRes,
		retryOnRs,
		1,
	}
}

// NewProxyRetryErrorWithRetryCount returns a ProxyRetryError with retryCount
// number of retries. This is the same as NewProxyRetryError with retryCount
// set to 1. If the retry fails (non-zero errorCode) retryCount number of times
// we will return the error back to the proxy.
func NewProxyRetryErrorWithRetryCount(msgToRetry Message, previousRes SimpleBSON, retryOnRs string, retryCount int) *ProxyRetryError {
	return &ProxyRetryError{
		msgToRetry,
		previousRes,
		retryOnRs,
		retryCount,
	}
}

type StackError struct {
	Message    string
	Stacktrace []string
}

func NewStackErrorf(messageFmt string, args ...interface{}) *StackError {
	return &StackError{
		Message:    fmt.Sprintf(messageFmt, args...),
		Stacktrace: stacktrace(),
	}
}

func (self *StackError) Error() string {
	return fmt.Sprintf("%s\n\t%s", self.Message, strings.Join(self.Stacktrace, "\n\t"))
}

func stacktrace() []string {
	ret := make([]string, 0, 2)
	for skip := 2; true; skip++ {
		_, file, line, ok := runtime.Caller(skip)
		if ok == false {
			break
		}

		ret = append(ret, fmt.Sprintf("at %s:%d", stripDirectories(file, 2), line))
	}

	return ret
}

func stripDirectories(filepath string, toKeep int) string {
	var idxCutoff int
	if idxCutoff = strings.LastIndex(filepath, "/"); idxCutoff == -1 {
		return filepath
	}

	for dirToKeep := 0; dirToKeep < toKeep; dirToKeep++ {
		switch idx := strings.LastIndex(filepath[:idxCutoff], "/"); idx {
		case -1:
			break
		default:
			idxCutoff = idx
		}
	}

	return filepath[idxCutoff+1:]
}

func extractError(rdr bsoncore.Document) error {
	if rdr == nil {
		return nil
	}

	var errmsg, codeName string
	var code int32
	var labels []string
	var ok bool
	var tv *description.TopologyVersion
	var wcError driver.WriteCommandError
	elems, err := rdr.Elements()
	if err != nil {
		return err
	}

	for _, elem := range elems {
		switch elem.Key() {
		case "ok":
			switch elem.Value().Type {
			case bson.TypeInt32:
				if elem.Value().Int32() == 1 {
					ok = true
				}
			case bson.TypeInt64:
				if elem.Value().Int64() == 1 {
					ok = true
				}
			case bson.TypeDouble:
				if elem.Value().Double() == 1 {
					ok = true
				}
			}
		case "errmsg":
			if str, okay := elem.Value().StringValueOK(); okay {
				errmsg = str
			}
		case "codeName":
			if str, okay := elem.Value().StringValueOK(); okay {
				codeName = str
			}
		case "code":
			if c, okay := elem.Value().Int32OK(); okay {
				code = c
			}
		case "errorLabels":
			if arr, okay := elem.Value().ArrayOK(); okay {
				elems, err := arr.Values()
				if err != nil {
					continue
				}
				for _, elem := range elems {
					if str, ok := elem.StringValueOK(); ok {
						labels = append(labels, str)
					}
				}

			}
		case "writeErrors":
			arr, exists := elem.Value().ArrayOK()
			if !exists {
				break
			}
			vals, err := arr.Values()
			if err != nil {
				continue
			}
			for _, val := range vals {
				var we driver.WriteError
				doc, exists := val.DocumentOK()
				if !exists {
					continue
				}
				if index, exists := doc.Lookup("index").AsInt64OK(); exists {
					we.Index = index
				}
				if code, exists := doc.Lookup("code").AsInt64OK(); exists {
					we.Code = code
				}
				if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
					we.Message = msg
				}
				wcError.WriteErrors = append(wcError.WriteErrors, we)
			}
		case "writeConcernError":
			doc, exists := elem.Value().DocumentOK()
			if !exists {
				break
			}
			wcError.WriteConcernError = new(driver.WriteConcernError)
			if code, exists := doc.Lookup("code").AsInt64OK(); exists {
				wcError.WriteConcernError.Code = code
			}
			if name, exists := doc.Lookup("codeName").StringValueOK(); exists {
				wcError.WriteConcernError.Name = name
			}
			if msg, exists := doc.Lookup("errmsg").StringValueOK(); exists {
				wcError.WriteConcernError.Message = msg
			}
			if info, exists := doc.Lookup("errInfo").DocumentOK(); exists {
				wcError.WriteConcernError.Details = make([]byte, len(info))
				copy(wcError.WriteConcernError.Details, info)
			}
			if errLabels, exists := doc.Lookup("errorLabels").ArrayOK(); exists {
				elems, err := errLabels.Values()
				if err != nil {
					continue
				}
				for _, elem := range elems {
					if str, ok := elem.StringValueOK(); ok {
						labels = append(labels, str)
					}
				}
			}
		case "topologyVersion":
			doc, ok := elem.Value().DocumentOK()
			if !ok {
				break
			}
			version, err := description.NewTopologyVersion(bson.Raw(doc))
			if err == nil {
				tv = version
			}
		}
	}

	if !ok {
		if errmsg == "" {
			errmsg = "command failed"
		}

		return driver.Error{
			Code:            code,
			Message:         errmsg,
			Name:            codeName,
			Labels:          labels,
			TopologyVersion: tv,
		}
	}

	if len(wcError.WriteErrors) > 0 || wcError.WriteConcernError != nil {
		wcError.Labels = labels
		if wcError.WriteConcernError != nil {
			wcError.WriteConcernError.TopologyVersion = tv
		}
		return wcError
	}

	return nil
}

func MessageMessageToBSOND(m *MessageMessage) (bson.D, *BodySection, error) {
	var bodySection *BodySection = nil
	for _, section := range m.Sections {
		if bs, ok := section.(*BodySection); ok {
			if bodySection != nil {
				return nil, nil, fmt.Errorf("OP_MSG should have only one body section!")
			}
			bodySection = bs
		}
	}
	if bodySection == nil {
		return nil, nil, fmt.Errorf("OP_MSG should have a body section!")
	}
	cmd, err := bodySection.Body.ToBSOND()
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to parse body section as bson: %v", err)
	}
	return cmd, bodySection, nil
}
