# User feedback analysis in Keboola Connection (KBC) using Geneea NLP platform.

Integration of the [Geneea API](https://api.geneea.com) with [Keboola Connection](https://connection.keboola.com).

This is a Docker container used for running user-feedback NLP analysis jobs in the KBC.
Automatically built Docker images are available at [Docker Hub Registry](https://hub.docker.com/r/geneea/kbc-feedback-analysis/).

The supported NLP analysis types are: `sentiment`, `entities`, `tags`, `relations`.

## Building a container
To build this container manually one can use:

```
git clone https://github.com/Geneea/kbc-feedback-analysis.git
cd kkbc-feedback-analysis
sudo docker build --no-cache -t geneea/kbc-feedback-analysis .
```

## Running a container
This container can be run from the Registry using:

```
sudo docker run \
--volume=/home/ec2-user/data:/data \
--rm \
geneea/kbc-feedback-analysis:latest
```
Note: `--volume` needs to be adjusted accordingly.
