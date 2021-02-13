package dfclient

import (
	"context"
	"fmt"

	eos "github.com/eoscanada/eos-go"
)

//Decoder Enables de decoding of table deltas
type Decoder struct {
	api      *eos.API
	abiCache map[eos.AccountName]*eos.ABI
}

//NewDecoder creates a new decoder
func NewDecoder(apiURL string) *Decoder {
	decoder := &Decoder{
		api:      eos.New(apiURL),
		abiCache: make(map[eos.AccountName]*eos.ABI),
	}
	return decoder
}

//Decode decodes table data
func (m *Decoder) Decode(account eos.AccountName, table eos.TableName, data []byte) ([]byte, error) {
	abi, err := m.GetABI(account)
	if err != nil {
		return nil, err
	}
	tableDef := abi.TableForName(table)
	if tableDef == nil {
		return nil, fmt.Errorf("Table: %v not found in ABI", table)
	}
	bytes, err := abi.DecodeTableRowTyped(tableDef.Type, data)
	if err != nil {
		return nil, err
	}
	logger.Trace().Msgf("Decoded data: %v", string(bytes))
	return bytes, nil
}

//GetABI gets the ABI for an account
func (m *Decoder) GetABI(account eos.AccountName) (*eos.ABI, error) {

	if _, ok := m.abiCache[account]; !ok {
		response, err := m.api.GetABI(context.Background(), account)
		if err != nil {
			return nil, err
		}
		m.abiCache[account] = &response.ABI
	}
	abi, _ := m.abiCache[account]
	return abi, nil
}
