Bloom Filter可以用于检测一个元素是否已经存在于集合中，它以允许一定的误判为代价，节省了大量的存储空间。

本文描述内容，是以Redis作为底层支撑，实现一个Bloom Filter。

需要提醒的是，Redis Stack当前已支持一些概率容器，包括Bloom Filter，Cuckoo Filter，参见：https://redis.io/docs/stack/bloom/。但是，Redis Stack并未包含在社区版的Redis中，本文的目的是使用基础的Bitmap结构，来实现一个简单的Bloom Filter。

因为使用Bitmap，所以本文描述的Bloom Filter受到Bitmap本身限制，最大容量为512M，即能存储2^32比特。

另外，本文描述的Bloom Filter暂不支持动态扩容。

本文使用了两个hash算法，分别是FNV1a和Murmur3。

Hash函数个数的计算以及支撑bit数组长度的计算，参见：[维基百科](https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions)

本文默认使用go-redis作为golang的redis驱动。