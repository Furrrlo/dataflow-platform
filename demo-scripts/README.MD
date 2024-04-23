
# How to run

- Have docker
- Build the docker images to your docker daemon using `gradle jibDockerBuild`
- Connect to a common network with all 3 computers 
- Edit your `C:\Windows\System32\drives\etc\hosts` file and add
  ```
  127.0.0.1 furrrlo
  127.0.0.1 fnufo
  127.0.0.1 tossso
  ```
  replacing the addresses with the ones of the other computers
- Navigate to the correct subfolder & run docker compose
  - `cd furrrlo && docker compose up` 
  - `cd fnufo && docker compose up` 
  - `cd tossso && docker compose up` 
- Find the `our-dataflow-coordinator` container id in `docker ps` and attach to 
  it using `docker attach <id>`