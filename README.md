# bus-test

```
pip install nats-py
sudo dpkg -i nats-0.0.35-amd64\ \(1\).deb

clear && docker run --rm -it --network=host nats:latest -js
clear && python worker.py
clear && python ms.py

```

```
nats s add archive --source progress --source workitems --retention=work --max-age=60m

nats s add progress-mirror --mirror progress
nats s add workitems-mirror --mirror workitems

nats sub dlwb.workitems
nats sub dlwb.progress.*.*

```