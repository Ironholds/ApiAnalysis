source("config.R")
source("functions.R")

# Run
lapply(c("geo","prefix","open","cirrus","language"), central)

# Quit
q(save = "no")
