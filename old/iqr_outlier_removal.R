remove_outliers_iqr <- function(x, multiple = 1.5, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- multiple * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  y
}

remove_outliers_iqr_mean <- function(x = x, w = w, multiple = 1.5, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- multiple * IQR(x, na.rm = na.rm)
  z <- w
  z[x < (qnt[1] - H)] <- 0
  z[x > (qnt[2] + H)] <- 0
  
  sum(x*z, na.rm = T)/sum(z, na.rm = T)
}



trim_outliers <- function(x, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.2, .8), na.rm = na.rm, ...)
  y <- x
  y[x < (qnt[1])] <- NA
  y[x > (qnt[2])] <- NA
  y
}

check_function <- function(x) {
  x[1]
  p <- length(x)
  return(x[p])
}


remove_rolling_iqr_outliers <- function(x, multiple = 1.5, na.rm = TRUE, ...) {
  qnt <- quantile(x, probs=c(.25, .75), na.rm = na.rm, ...)
  H <- multiple * IQR(x, na.rm = na.rm)
  return(ifelse(x[1] > (qnt[1] - H - 0.02) & x[1] < (qnt[2] + H + 0.02), x[1], NA))
}
