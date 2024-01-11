/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.admin.compaction;

import static java.lang.Long.max;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * <p>
 * A compaction configurer that extends @CompressionConfigurer.java and adds the ability to control
 * and configurer how Erasure Codes work.
 *
 * <ul>
 * <li>Set {@code table.compaction.configurer.opts.erasure.code.size.conversion} to a size in bytes.
 * The suffixes K,M,and G can be used. This is the minimum file size to allow conversion to erasure
 * code. If a file is below this size it will be tripple replicated.
 * <li>Set {@code table.compaction.configurer.opts.erasure.code.minimize.nn.overhead} to true or
 * false. When this option is set to true it will erasure code files only of the number of
 * datablocks + number of parityblocks <= the number of replicated datablocks. The computation is
 * max( (size_of(file)/hdfs_blocksize, 1)*replication_factor. If this is less than
 * sum(datablocks,parityblocks) then the file is 3x replicated.
 * <li>Set {@code table.compaction.configurer.opts.erasure.code.override.to.replication } to true or
 * false. When this is set to true override ec setting.
 * <li>Set {@code table.compaction.configurer.opts.erasure.code.bypass } to true or false. Setting
 * this to true will bypass erasure codes if the are configured. This options allows initiated
 * compactions to bypass logic in this class if needed.
 * </ul>
 *
 * This class provides several options for influencing how a file gets erasure coded. A user can
 * decide to minimize impact on the namenode by only encoding a file when it's ecgroup generates
 * less namenode objects than replication. A user can set a minimum file size to encode. Files with
 * a size less than the specified value will not be encoded. A user can also initiate a compaction
 * and and bypass the logic in this class. Lastly a user can set an ec policy on a set of
 * directories (a section of a table) and use the logic in this class only on those directories.
 *
 * </p>
 *
 *
 * Header from @CompressionConfigurer.java.
 *
 * <p>
 * To use compression type CL for large files and CS for small files, set the following table
 * properties.
 *
 * <ul>
 * <li>Set {@code table.compaction.configurer.opts.large.compress.threshold } to a size in bytes.
 * The suffixes K,M,and G can be used. When the inputs exceed this size, the following compression
 * is used.
 * <li>Set {@code  table.compaction.configurer.opts.large.compress.type} to CL.
 * <li>Set {@code table.file.compress.type=CS}
 * </ul>
 *
 * <p>
 * With the above config, minor compaction and small compaction will use CS for compression.
 * Everything else will use CL. For example CS could be snappy and CL could be gzip. Using a faster
 * compression for small files and slower compression for larger files can increase ingestion
 * throughput without using a lot of extra space.
 *
 * @since 2.1.0
 */
public class ErasureCodeConfigurer extends CompressionConfigurer {
  private static final Logger LOG = LoggerFactory.getLogger(ErasureCodeConfigurer.class);
  public static final String ERASURE_CODE_SIZE = "erasure.code.size.conversion";
  public static final String ERASURE_CODE_MIN_NAMENODE_OVERHEAD =
      "erasure.code.minimize.nn.overhead";
  // TODO this is not used
  public static final String ERASURE_CODE_OVERRIDE_TO_REP = "erasure.code.override.to.replication";
  public static final String BYPASS_ERASURE_CODES = "erasure.code.bypass";
  private String ecPolicyName = null;
  private Long ecSize;
  private Boolean minNamenodeOverhead = false;
  private Boolean byPassEC = false;
  private Long hdfsBlockSize;
  private Long numReplication;

  @Override
  public void init(InitParameters iparams) {
    var options = iparams.getOptions();
    var config = iparams.getEnvironment().getConfiguration(iparams.getTableId());
    String ecSize = options.get(ERASURE_CODE_SIZE);
    Boolean useEC = Boolean.parseBoolean(config.get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));
    Boolean bypass = Boolean.parseBoolean(options.get(BYPASS_ERASURE_CODES));
    String blockSize = config.get(Property.TABLE_FILE_BLOCK_SIZE.getKey());
    String reps = config.get(Property.TABLE_FILE_REPLICATION.getKey());
    String policyName = config.get(Property.TABLE_ERASURE_CODE_POLICY.getKey());
    Boolean orEC = false;// Boolean.parseBoolean(options.get(this.overrideToRepOnly));

    this.ecPolicyName = policyName;
    this.byPassEC = bypass;

    if (useEC) {
      // Default min ec block size for encoding to blocksize if not set.
      if (ecSize != null) {
        this.ecSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(ecSize);
      } else {
        this.ecSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(blockSize);
      }
      // Enable namenode memory optimization for ec
      this.minNamenodeOverhead =
          Boolean.parseBoolean(options.get(ERASURE_CODE_MIN_NAMENODE_OVERHEAD));
      if (this.minNamenodeOverhead && blockSize != null) {
        // TODO the defautl for this is zero, can cause deivide by zero.. also when zero, may fall
        // back to hdfs setting
        this.hdfsBlockSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(blockSize);
        this.numReplication = Long.parseLong(reps, 10);
      }

    }

    super.init(iparams);

  }

  @Override
  public Overrides override(InputParameters params) {

    long inputsSum =
        params.getInputFiles().stream().mapToLong(CompactableFile::getEstimatedSize).sum();

    // Allow for user initiated compactions to pass an options to bypass EC.
    if (this.byPassEC) {
      return new Overrides(Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "False"));
    }

    boolean ecFile = false;
    long total_file_blocks_with_rep = 0;

    if (this.minNamenodeOverhead) {
      /**
       * HDFS stores ec files as an ec group. For an (m,n) scheme a group consists of m+n objects in
       * the namenode. This optimization will only encode a file *IF* the file is > than the minimum
       * ec file size and the m+n blocks are < total_file_blocks*replication_number. For example. A
       * 1GB file with hdfs blocksize of 512MB. This file will have 2 hdfs blocks X 3 replication.
       * That's a total of 6 objects in the namenode. If we are using a (6,3) ec policy, that would
       * store 6+3 = 9 objects in the name node. In this example the 1GB file would not be encoded.
       * If we had the same 1 GB file, but a 256MB hdfs blocksize, then there would be 4 hdfs blocks
       * X 3 rep which would store 12 objects in the namenode. In comparison for a (6,3) scheme
       * there would only be 9 objects stored in the namenode. So for a 256MB block size we would
       * encode a 1 GB file using the namenode optimization. Note: we are using the default HDFS
       * blocksize as the ecSize in this comparison.
       */

      // Compute the blocks from current ec scheme (m,n) = m+n. If there is a failure in
      // return #Integer.MAX_VALUE if fail to parse scheme
      long total_ec_blocks = computeBlocksInECGroup(this.ecPolicyName);

      // compute number of hdfs blocks * replication factor
      total_file_blocks_with_rep = (max(inputsSum / this.hdfsBlockSize, 1L)) * this.numReplication;

      // TODO set to trace,debug,or remove
      LOG.info("total_ec_blocks:{} total_file_blocks_with_rep:{} inputSum:{} ecSize:{}",
          total_ec_blocks, total_file_blocks_with_rep, inputsSum, ecSize);

      ecFile = ((total_file_blocks_with_rep >= total_ec_blocks) && (inputsSum >= this.ecSize));
    } else {
      // Compare projected compacted file size with minimum ec file size. If smaller do not encode.
      ecFile = (inputsSum >= this.ecSize);
    }

    // call compressionconfigurer to determine compression settings
    Map<String,String> overs = new HashMap<>(super.override(params).getOverrides());

    overs.put(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), Boolean.toString(ecFile));

    return new Overrides(overs);
    // Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), Boolean.toString(ecFile)));
  }

  /**
   *
   * @param ecPolicy the hdfs name of erasure code policy. The name encodes the number of data
   *        blocks and parity blocks. We are not checking the accuracy of the policy name. We just
   *        need the block count to see if we are going to ec the file or not.
   * @return total blocks
   */
  private long computeBlocksInECGroup(String ecPolicy) {
    long numBlocks = 0;
    int dbIdx = 0;
    int pbIdx = 0;
    // policy name is structured as
    // (coder|coder-legacy)-(data_blocks)-(parity_blocks)-(celsize)

    String[] parts = ecPolicy.split("-");

    if (parts.length == 4) {
      dbIdx = 1;
      pbIdx = 2;
    } else if (parts.length == 5) {
      // rc-legacy (used for legacy rc coder)
      dbIdx = 2;
      pbIdx = 3;
    } else {
      throw new IllegalArgumentException("Unexpected EC policy " + ecPolicy);
    }
    try {
      // TODO does total_ec_blocks need to be a function of the file size?
      numBlocks = Long.parseLong(parts[dbIdx]) + Long.parseLong(parts[pbIdx]);
    } catch (NumberFormatException nfe) {
      LOG.warn("Could not parse ec scheme correctly: " + ecPolicy + " can not use "
          + ERASURE_CODE_MIN_NAMENODE_OVERHEAD + " correctly. Disabling optimization.");
      numBlocks = Integer.MAX_VALUE;
    }

    return numBlocks;
  }

}
