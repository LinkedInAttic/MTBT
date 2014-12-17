 int flushPoolSize = 2;
    int compactionPoolSize = 10;

    re.setBackgroundThreads(flushPoolSize, RocksEnv.FLUSH_POOL);
    re.setBackgroundThreads(compactionPoolSize, RocksEnv.COMPACTION_POOL);    Options options = new Options();
      options.setCompactionStyle(CompactionStyle.UNIVERSAL);
      //options.setCacheNumShardBits(2);

      String dbName = itr.next();
      String dbPath = folderPath + "/" + dbName;

      if(dbName.equals("db46") || dbName.equals("db47"))
      {
        options.setCreateIfMissing(true)
        .setWriteBufferSize(64 * SizeUnit.MB)
        .setStatsDumpPeriodSec(20)
        .setTargetFileSizeBase((int) (64 * SizeUnit.MB))
        .setMaxBackgroundFlushes(flushPoolSize)
        .setMaxBackgroundCompactions(compactionPoolSize)
        .setCacheSize(64 * SizeUnit.MB)
        .setSoftRateLimit(50)
        .setBlockSize(64 * SizeUnit.KB)
        .setFilter(filter);
        //.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
      }
      else
      {
        options.setCreateIfMissing(true)
        .setWriteBufferSize(64 * SizeUnit.MB)
        .setStatsDumpPeriodSec(20)
        .setTargetFileSizeBase((int) (64 * SizeUnit.MB))
        .setMaxBackgroundFlushes(flushPoolSize)
        .setMaxBackgroundCompactions(compactionPoolSize)
        .setCacheSize(64 * SizeUnit.MB)
        .setSoftRateLimit(50)
        .setBlockSize(8 * SizeUnit.KB)
        .setFilter(filter);
        //.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
      }
      m_optionList.add(options);
