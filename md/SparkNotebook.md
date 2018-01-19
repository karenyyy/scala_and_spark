## Spark Notebook setup (linux)

### install docker on ubuntu

- First, add the GPG key for the official Docker repository to the system:
  
  > curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  
- Add the Docker repository to APT sources:
  
  > sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  
  
- Next, update the package database with the Docker packages from the newly added repo:
  
  > sudo apt-get update
  
- Make sure you are about to install from the Docker repo instead of the default Ubuntu 16.04 repo:
  
  > sudo apt-cache policy docker-ce
  
    - You should see output similar to the follow:
      
           Output of apt-cache policy docker-ce
              docker-ce:
                Installed: (none)
                Candidate: 17.03.1~ce-0~ubuntu-xenial
                Version table:
                   17.03.1~ce-0~ubuntu-xenial 500
                      500 https://download.docker.com/linux/ubuntu xenial/stable amd64 Packages
                   17.03.0~ce-0~ubuntu-xenial 500
                      500 https://download.docker.com/linux/ubuntu xenial/stable amd64 Packages
                      
- Finally, install Docker:
  
  > sudo apt-get install -y docker-ce
  
  
  
- Docker should now be installed, the daemon started, and the process enabled to start on boot. Check that it's running:
  
  > sudo systemctl status docker
  
    - The output should be similar to the following, showing that the service is active and running:
      
          Output
              ● docker.service - Docker Application Container Engine
                 Loaded: loaded (/lib/systemd/system/docker.service; enabled; vendor preset: enabled)
                 Active: active (running) since Sun 2016-05-01 06:53:52 CDT; 1 weeks 3 days ago
                   Docs: https://docs.docker.com
      
      
## install spark notebook with docker

[Here](http://spark-notebook.io)


> docker pull andypetrella/spark-notebook:0.7.0-scala-2.10.6-spark-2.1.1-hadoop-2.7.2-with-hive


> docker run -p 9001:9001 andypetrella/spark-notebook:0.7.0-scala-2.10.6-spark-2.1.1-hadoop-2.7.2-with-hive


- then go to `localhost:9001`, see if it is working