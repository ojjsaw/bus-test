# bus-test

```
pip install nats-py
sudo dpkg -i nats-0.0.35-amd64\ \(1\).deb

clear && docker run --rm -it --network=host nats:latest -js
clear && python worker.py
clear && python ms.py
clear && python ms2.py


500 lines download
30 lines convert
500 lines benchmark max (detailed counters + benchmark results)

less than 1MB for detailed counters in single message
500 lines max

benchmark_app -m /workspaces/bus-test/public/aclnet/FP32/aclnet.xml -d CPU -t 5
benchmark_app -m /workspaces/bus-test/public/aclnet/FP32/aclnet.xml -d CPU -t 5 --report_type detailed_counters

sudo apt-get update && export DEBIAN_FRONTEND=noninteractive && sudo apt-get -y install --no-install-recommends libgl1
```

```
nats sub "dlwb.>"
```