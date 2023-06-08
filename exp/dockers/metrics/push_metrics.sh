#/bin/bash
while true;
do
	echo '# HELP probe_dns_lookup_time_seconds Returns the time taken for probe dns lookup in seconds' > metric_temp
	echo '# TYPE probe_dns_lookup_time_seconds gauge' >> metric_temp
	echo `curl -o /dev/null -s -w "probe_dns_lookup_time_seconds  %{time_namelookup}\n" https://www.baidu.com` >> metric_temp

	echo '# HELP probe_http_status_code Response HTTP status code'  >> metric_temp
	echo '# TYPE probe_http_status_code gauge' >> metric_temp
	echo "probe_http_status_code `curl -I -m 10 -o /dev/null -s -w %{http_code} https://www.baidu.com`" >> metric_temp

	echo '# HELP probe_duration_seconds Returns how long the probe took to complete in seconds' >> metric_temp
	echo '# TYPE probe_duration_seconds gauge' >> metric_temp
	echo `curl -o /dev/null -s -w "probe_duration_seconds  %{time_total}\n" "https://www.baidu.com"` >> metric_temp

	#push metric to prometheus
	cat metric_temp| curl -s --data-binary @- http://localhost:9091/metrics/job/some_job/instance/ip-192.168.200.200;
	date;
	sleep 15;
done
