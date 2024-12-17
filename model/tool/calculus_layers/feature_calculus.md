

#### Current

1. max
2. min
3. mean
4. median
5. mode
6. std
7. sum
8. signed_sum (sum(sign(data), N))
9. mean_diff (data - mean(N))
10. skew
11. shift
12. shift_diff  (data - data.shift(N))
13. shift_div   (data / data.shift(N))
14. std_mean_diff  (mean_diff / std)
15. cummax_periodical   (max(cumsum(N)))
16. cummin_periodical   (min(cumsum(N)))
17. cummax_tot   (max(cumsum), N)
18. cummin_tot   (min(cumsum), N)
19. exclude_std_mean_diff   (outside 1x std)
20. double_exclude_std_mean_diff  (outside 2x std)
21. mean_div   (data / mean(N))
22. momentum   (mean_diff / mean)
23. diff_max   (data - max(N))
24. diff_min   (data - min(N))
25. rank   (argsort)
26. mean_angular  
27. mad (mean)
28. fat_tail (mean)
29. log_div_max   log(e)(max(N) / data)
30. log_div_min   log(e)(min(N) / data)
31. log_mean_div  log(e)(mean_div(N))


### drop-out
1. kurt   (kurt - 3)
2. abs_sum   (sum(abs(data), N))
