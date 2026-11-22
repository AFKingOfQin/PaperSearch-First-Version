# AI-generated figure (Codex): draw the individual repeated measurements using base R.
args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 2)
trials <- read.csv(args[1])
stopifnot(nrow(trials) == 27)
png(args[2], width = 1680, height = 660, res = 150)
par(mfrow = c(1, 2), mar = c(4.5, 9.2, 3.6, 1.4), family = "sans", las = 1)
panel <- function(modes, labels, field, unit, title) {
    observations <- lapply(modes, function(m) trials[trials$mode == m, field])
    stopifnot(all(lengths(observations) == 3))
    bounds <- range(unlist(observations))
    plot(NA, xlim = c(0, bounds[2] * 1.10), ylim = c(0.55, length(modes) + 0.45),
         yaxt = "n", ylab = "", xlab = unit, bty = "n", main = title)
    rows <- rev(seq_along(labels))
    axis(2, at = rows, labels = labels, tick = FALSE)
    for (i in seq_along(rows)) {
        x <- observations[[i]]
        segments(min(x), rows[i], max(x), rows[i], col = "#39768a", lwd = 2)
        points(x, rows[i] + c(-0.07, 0, 0.07), pch = 21,
               col = "#13516b", bg = "#d6eaf0", cex = 1.05)
        points(median(x), rows[i], pch = 18, col = "#163443", cex = 1.15)
    }
}
panel(c("full", "incremental", "bypass", "derived2"),
      c("Rebuild", "Update", "Cache bypass", "2 derived workers"), "elapsed_seconds",
      "Seconds to published snapshot", "Index publication")
panel(c("workers1", "workers2", "workers3", "before", "after"),
      c("1 worker", "2 workers", "3 workers", "Before SQL commit", "After SQL commit"),
      "events_per_second", "Events per second", "Ingestion and recovery")
dev.off()
