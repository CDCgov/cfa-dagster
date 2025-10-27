args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  print(paste0("Hello from R!"))
} else {
  disease <- args[1]
  print(paste0("Hello, ", disease, " from R!"))
}

# Generate a random number between 1 and 20
random_number <- sample(1:20, 1)
# Write the random number to output.txt
write(random_number, "output.txt")
print(paste0("Generated random number: ", random_number))
