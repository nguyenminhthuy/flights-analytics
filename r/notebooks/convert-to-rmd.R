convert_r_to_rmd <- function(input, output) {
  # Read original R file
  lines <- readLines(input, warn = FALSE)
  
  # Create R Markdown structure
  rmd <- c(
    "---",
    "title: \"Auto Converted R Script\"",
    "output: html_document",
    "---",
    "",
    "```{r}",
    lines,
    "```"
  )
  
  # Write to .Rmd
  writeLines(rmd, output)
}

# ====== RUN CONVERSION ======
convert_r_to_rmd(
  input  = "flights-eda.R",
  output = "flights-rmd.Rmd"
)
