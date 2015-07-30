###HOW-TO RUN
`./activator run`

###HOW-TO OPEN IN SUBLIME
`./activator gen-sublime`
`subl ./*.sublime-project` || manually open `.sublime-project` file.

###Problem
I want to understand how `PoolingOptions::ConnectionsPerHost` and `PoolingOptions::NewConnectionThreshold` works.
I expect them to add new connection when NewConnectionThreshold is reached, untils MaxConnectionsPerHost reached, but looks like number of connections stays at `coreConnectionsPerHost`.
I monitor number of connections via `watch -d -n0 "netstat -atnp | awk 'NR <= 2 || /9042/'"` on cassandra side.
