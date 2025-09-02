# hft data reader/writer

writes different forms of hft data to disk in binary format
uses an Structure of Arrays format, with 1 column per field

ex) say we want to store level 2 data, 4 fields

uint64_t timestamp (ns unix epoch)


uint32_t price (multiply by however many powers of 10 to remove decimals)


float qty (sizes are usually fractional for crypto, dont think it is wise to convert them to ints, change to uint32_t for tradfi)


uint8_t side (1 = bid, 0 = ask)

random estimate of 1 million events per day (1 << 20)
first we have a 256 byte header that gives information about the data
we preallocate (8 bytes * 1'000'000) + (4 bytes * 1'000'000) + (4 bytes * 1'000'000) + (2 bytes * 1'000'000) + 256byte header

now we need pointers to the data, accessed via array indexing

*ts = offset: 255 (first data point after the header)


*price = offset: (255 + (8 bytes * 1'000'000)),


*qty = offset: (255 + (8 bytes * 1'000'000) + (4 bytes * 1'000'000))


*side = offset: (255 + (8 bytes * 1'000'000) + (4 bytes * 1'000'000) + (4 bytes * 1'000'000))


the main advantage of this is the ability to construct factor trees for research/tweaking of trading models

on a first pass, we read this data into the orderbook, calculate and store factors every X ticks (whatever time interval you prefer)
then, we write those factors to disk

on any subsequent passes, we can avoid having to read level 2/3 orderbook data into the book again, and can directly pass the factors to our strategy code, this saves a LOT of time when it comes to backtesting intraday data over long ranges
