package nebula

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	// Blank import required to register driver
	"flashcat.cloud/categraf/inputs"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/types"
	"github.com/emirpasic/gods/lists/singlylinkedlist"
	nebula_client "github.com/vesoft-inc/nebula-go/v3"
)

type MetricsData struct {
	Name      string
	Value     float64
	LabelPair map[string]string
}

const (
	inputName = "nebula"
)

var labelReplacer = strings.NewReplacer("-", "_", ".", "_", " ", "_", "/", "_")

type Nebula struct {
	config.PluginConfig
	Instances []Instance `toml:"instances"`
	Sqlquerys []Sqlquery `toml:"sqlquerys"`
	instances *singlylinkedlist.List
	sqlquerys *singlylinkedlist.List
}
type Sqlquery struct {
	Env            string             `toml:"env"`
	Address        string             `toml:"address"`
	Port           int                `toml:"port"`
	Username       string             `toml:"username"`
	Password       string             `toml:"password"`
	UseHTTP2       bool               `toml:"useHTTP2"`
	Spacename      string             `toml:"spacename"`
	Sql_script     string             `toml:"sql_script"`
	Metrics_name   string             `toml:"metrics_name"`
	Labels_names   []string           `toml:"labels_names"`
	Values_names   []string           `toml:"values_names"`
	Values_mapping map[string]float64 `toml:"values_mapping"`
}

type Instance struct {
	Env         string   `toml:"env"`
	List        []string `toml:"list"`
	Metricsport int      `toml:"metricsport"`
	Urlsuffix   string   `toml:"urlsuffix"`
	Role        string   `toml:"role"`
}

// Initialize logger
var log_client = nebula_client.DefaultLogger{}

func init() {
	inputs.Add(inputName, func() inputs.Input {
		return &Nebula{}
	})
}

func (ins *Nebula) Init() error {
	if len(ins.Instances) == 0 {
		return errors.New("no instance configured")
	}

	ins.instances = singlylinkedlist.New()

	for _, instanceOption := range ins.Instances {
		ins.instances.Add(&instanceOption)
	}
	ins.sqlquerys = singlylinkedlist.New()
	for _, sqlqueryOption := range ins.Sqlquerys {
		ins.sqlquerys.Add(&sqlqueryOption)
	}

	return nil
}

func (pt *Nebula) Clone() inputs.Input {
	return &Nebula{}
}

func (pt *Nebula) Name() string {
	return inputName
}

// func (pt *Nebula) GetInstances() []inputs.Instance {
// 	ret := make([]inputs.Instance, len(pt.Instances))
// 	for i := 0; i < len(pt.Instances); i++ {
// 		ret[i] = pt.Instances[i]
// 	}
// 	return ret
// }

// 拼接metrics url 地址,instance级别
func (ins *Instance) InstanceMetricUrl() map[string]interface{} {
	hostNames := ins.List
	var serverurl = make(map[string]string)
	var urlList = make(map[string]interface{})
	for _, hostName := range hostNames {
		serverurl[hostName] = fmt.Sprintf("http://%s:%d%s", hostName, ins.Metricsport, ins.Urlsuffix)
	}
	urlList[ins.Env] = serverurl
	// 验证程序输出
	fmt.Fprintln(log.Writer(), "the urlList is ", urlList)
	return urlList
}

// 获取urllist列表的指标值,instance级别返回值为{"hostname":"data",hostname2:"data2"}
func (ins *Instance) GetData(urlList map[string]interface{}) (map[string]string, error) {
	var f_all = make(map[string]string)
	for _, serverurl := range urlList {
		for hostname, url := range serverurl.(map[string]string) {
			resp, err := http.Get(url)
			if err != nil {
				// return nil, fmt.Errorf("get data from %s failed: %v", requestURL, err)
				continue
			}
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				// return nil, fmt.Errorf("read data from %s failed: %v", requestURL, err)
				continue
			}
			// fmt.Println("the f is ", f)
			f_all[hostname] = string(data)
		}

	}
	// fmt.Println("f_all的内容为：", f_all)
	// fmt.Println("the f_all is", f_all)
	return f_all, nil
}

// 指标切片，instance级别
func (ins *Instance) FetchData(data map[string]string) ([]MetricsData, error) {
	dataList := make([]MetricsData, 0)
	for hostname, respdata := range data {
		lines := strings.Split(respdata, "\n") // 按行拆分
		for _, line := range lines {
			// 跳过注释行和空行
			if strings.HasPrefix(line, "#") || line == "" {
				// formattedLines = append(formattedLines, line)
				continue
			}
			parts := strings.Split(line, "=")
			if len(parts) < 2 {
				// 跳过无效行
				continue
			} else if len(parts) > 2 {
				metric, value, interval, functype, labelKey, labelValue, ok := parseComplexMetric(line)
				if !ok {
					continue
				}
				labels := make(map[string]string)
				labels["hostname"] = hostname
				labels["interval"] = interval
				labels["functype"] = functype
				labels[labelKey] = labelValue
				metricsData := MetricsData{}
				metricsData.Name = metric
				floatValue, _ := strconv.ParseFloat(value, 64)
				metricsData.Value = floatValue
				metricsData.LabelPair = labels
				dataList = append(dataList, metricsData)
				continue
			}
			metric, value := parts[0], parts[1]
			metricParts := strings.Split(metric, ".")
			if len(metricParts) < 3 {
				continue // 不符合预期格式则跳过
			}
			baseName := strings.Join(metricParts[:len(metricParts)-2], "_") // 将名称部分合并
			interval := metricParts[len(metricParts)-1]
			metricType := metricParts[len(metricParts)-2]
			labels := make(map[string]string)
			labels["hostname"] = hostname
			labels["interval"] = interval
			labels["functype"] = metricType
			metricsData := MetricsData{}
			metricsData.Name = baseName
			floatValue, _ := strconv.ParseFloat(value, 64)
			metricsData.Value = floatValue
			metricsData.LabelPair = labels
			dataList = append(dataList, metricsData)
		}

	}
	return dataList, nil
}

func (sql *Sqlquery) FetchForNSql(nsql string) ([]MetricsData, error) {
	dataList := make([]MetricsData, 0)
	hostAddress := nebula_client.HostAddress{Host: sql.Address, Port: sql.Port}
	config, err := nebula_client.NewSessionPoolConf(
		sql.Username,
		sql.Password,
		[]nebula_client.HostAddress{hostAddress},
		sql.Spacename,
		nebula_client.WithHTTP2(sql.UseHTTP2),
	)
	if err != nil {
		fmt.Sprintf("failed to create session pool config, %s", err.Error())
		return dataList, err
	}
	// create session pool
	sessionPool, err := nebula_client.NewSessionPool(*config, nebula_client.DefaultLogger{})
	if err != nil {
		fmt.Sprintf("failed to initialize session pool, %s", err.Error())
		return dataList, err
	}
	defer sessionPool.Close()
	checkResultSet := func(prefix string, res *nebula_client.ResultSet) {
		if !res.IsSucceed() {
			fmt.Sprintf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, res.GetErrorCode(), res.GetErrorMsg())
			return
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	var resultSet *nebula_client.ResultSet
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		resultSet, err = sessionPool.Execute(nsql)
		if err != nil {
			fmt.Print(err.Error())
			return
		}
		checkResultSet(nsql, resultSet)
		// personList: [{Bob 10 97.2} {Bob 10 80} {Bob 10 70}]
	}(&wg)
	wg.Wait()
	colNames := resultSet.GetColNames()
	fmt.Printf("column names: %s\n", strings.Join(colNames, ", "))

	for i := 0; i < resultSet.GetRowSize(); i++ {
		record, err := resultSet.GetRowValuesByIndex(i)
		if err != nil {
			fmt.Print(err.Error())
			return dataList, err
		}
		labels := make(map[string]string)
		labels["env"] = sql.Env
		// host, err := record.GetValueByIndex(0)
		if len(sql.Labels_names) == 0 {
			labels["host"] = sql.Address
		}
		for _, label := range sql.Labels_names {
			label_value, err := record.GetValueByColName(label)
			if err != nil {
				fmt.Print(err.Error())
				return dataList, err
			}
			labels[strings.ToLower(label)] = strings.Trim(label_value.String(), `"`)
		}

		for _, valuename := range sql.Values_names {
			value_spr, err := record.GetValueByColName(valuename)
			if err != nil {
				fmt.Print(err.Error())
				continue
			}
			metricsData := MetricsData{}
			metricsData.Name = "nebula_sqlquery_" + sql.Metrics_name + "_" + labelReplacer.Replace(strings.ToLower(valuename))
			value_string := value_spr.String()
			value, err := strconv.ParseFloat(value_string, 64)
			if err != nil {
				strValue_a := strings.Trim(value_string, `"`)
				if _, ok := sql.Values_mapping[strValue_a]; ok {
					value = sql.Values_mapping[strValue_a]
				} else {
					fmt.Println("The value indicator is not of numeric type, and the mapping table matching failed. The failed matching items are", strValue_a, "the sql is ", nsql)
					continue
				}
			}
			metricsData.Value = value
			metricsData.LabelPair = labels
			dataList = append(dataList, metricsData)
		}
	}
	return dataList, nil
}

func parseComplexMetric(line string) (metric, value string, interval, functype, labelKey, labelValue string, ok bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", "", "", "", "", "", false
	}

	// 1. 找到最后一个 = 号，分割键值
	lastEqual := strings.LastIndex(line, "=")
	if lastEqual == -1 {
		return "", "", "", "", "", "", false
	}

	keyPart := strings.TrimSpace(line[:lastEqual])
	value = strings.TrimSpace(line[lastEqual+1:])

	// 2. 解析键部分：slow_query_latency_us{space=bigmeta052}.p999.3600
	// 首先解析标签部分 {space=bigmeta052}
	labelStart := strings.Index(keyPart, "{")
	labelEnd := strings.Index(keyPart, "}")

	var labels map[string]string
	var baseMetric string

	if labelStart != -1 && labelEnd != -1 && labelStart < labelEnd {
		// 提取基础指标名
		baseMetric = strings.TrimSpace(keyPart[:labelStart])

		// 提取标签部分
		labelStr := keyPart[labelStart+1 : labelEnd]
		labels = parseLabels(labelStr)

		// 获取剩余部分：.p999.3600
		remaining := strings.TrimSpace(keyPart[labelEnd+1:])

		// 3. 解析 .p999.3600 部分
		if remaining != "" && strings.HasPrefix(remaining, ".") {
			parts := strings.Split(strings.TrimPrefix(remaining, "."), ".")
			if len(parts) >= 1 {
				functype = parts[0] // p999
			}
			if len(parts) >= 2 {
				interval = parts[1] // 3600
			}
		}
	} else {
		// 如果没有标签，尝试直接解析
		parts := strings.Split(keyPart, ".")
		if len(parts) >= 1 {
			baseMetric = parts[0]
		}
		if len(parts) >= 2 {
			functype = parts[1]
		}
		if len(parts) >= 3 {
			interval = parts[2]
		}
	}

	// 4. 如果没有标签，尝试其他格式解析
	if len(labels) == 0 {
		labels = extractLabelsFromString(keyPart)
	}

	// 5. 获取第一个标签的key和value
	for k, v := range labels {
		labelKey = k
		labelValue = v
		break // 只取第一个标签
	}

	metric = baseMetric
	ok = true

	return
}

func parseLabels(labelStr string) map[string]string {
	labels := make(map[string]string)

	// 按逗号分割多个标签
	pairs := strings.Split(labelStr, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// 分割 key=value
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			// 去除可能的引号
			val = strings.Trim(val, "\"")
			val = strings.Trim(val, "'")
			labels[key] = val
		}
	}

	return labels
}

func extractLabelsFromString(str string) map[string]string {
	labels := make(map[string]string)

	// 尝试从字符串中提取类似 key=value 的模式
	re := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)=([^.,{}"'\s]+)`)
	matches := re.FindAllStringSubmatch(str, -1)

	for _, match := range matches {
		if len(match) >= 3 {
			labels[match[1]] = match[2]
		}
	}

	return labels
}

func MapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func (ins *Nebula) Gather(slist *types.SampleList) {
	if len(ins.Instances) == 0 && len(ins.Sqlquerys) == 0 {
		return
	}
	waitMetrics := new(sync.WaitGroup)
	instanceCollect := func(instance *Instance) {

		defer waitMetrics.Done()
		data, getDataErr := instance.GetData(instance.InstanceMetricUrl())
		if getDataErr != nil {
			log.Printf("E! Failed to get data from %s: %v", instance.Env, getDataErr)
			return
		}

		keys := MapKeys(data)
		for _, k := range instance.List {
			if !contains(keys, k) {
				slist.PushSample(inputName, "up", 0, map[string]string{
					"env":  instance.Env,
					"role": instance.Role,
					"host": k,
				})
			}
		}
		for _, k := range keys {
			slist.PushSample(inputName, "up", 1, map[string]string{
				"env":  instance.Env,
				"role": instance.Role,
				"host": k,
			})
		}

		res, fetchDataErr := instance.FetchData(data)
		if fetchDataErr != nil {
			log.Printf("E! Failed to fetch data from %s: %v", instance.Env, fetchDataErr)
			return
		}
		for _, metricsData := range res {
			labels := metricsData.LabelPair
			labels["env"] = instance.Env
			labels["role"] = instance.Role

			slist.PushSample(inputName,
				metricsData.Name,
				metricsData.Value,
				labels)
		}
	}

	sqlqueryCollect := func(sql *Sqlquery) {
		defer waitMetrics.Done()
		res, fetchDataErr := sql.FetchForNSql(sql.Sql_script)
		if fetchDataErr != nil {
			log.Printf("E! Failed to fetch data from %s: %v", sql.Env, fetchDataErr)
			return
		}
		for _, metricsData := range res {
			labels := metricsData.LabelPair
			slist.PushSample(inputName,
				metricsData.Name,
				metricsData.Value,
				labels)
		}
	}

	for iter := ins.instances.Iterator(); iter.Next(); {
		waitMetrics.Add(1)
		go instanceCollect(iter.Value().(*Instance))
	}

	for iter2 := ins.sqlquerys.Iterator(); iter2.Next(); {
		waitMetrics.Add(1)
		go sqlqueryCollect(iter2.Value().(*Sqlquery))
	}
	waitMetrics.Wait()

}
