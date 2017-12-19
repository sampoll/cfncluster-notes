library(gQTLstats)
library(ldblock)
library(VariantAnnotation)
library(geuvPack)
library(batchtools)

data(geuFPKM)
ss <- stack1kg()
v17 <- ss@files[[17]]
someGenes <- c("ORMDL3","GSDMB", "IKZF3", "MED24", "CSF3", "ERBB2",
               "GRB7", "MIEN1", "GSDMA", "THRA", "MSL1")
se17 <- geuFPKM[ which(rowData(geuFPKM)$gene_name %in% someGenes),]

n <- 10
vr0 <- 39.5e6
vr1 <- 40.5e6
v <- seq(vr0, vr1, length=n+1)
vl <- zipup(v[1:n],v[2:(n+1)]-1)

run.job <- function(v, se, vcf)  {
  library(gQTLstats)
  library(ldblock)
  library(VariantAnnotation)
  library(geuvPack)

  vr <- GRanges("17", IRanges(start=v[1], end=v[2], names=c("range")))
  results <- AllAssoc(se, vcf, vr)
}

concat.job <- function(gr1, gr2)  {
  gr <- c(gr1,gr2)
}

