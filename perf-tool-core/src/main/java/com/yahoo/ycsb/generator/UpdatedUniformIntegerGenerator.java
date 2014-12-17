package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

/**
 * Generates integers uniformly at random from 0 (inclusive) to last inserted integer (inclusive).
 */
public class UpdatedUniformIntegerGenerator extends IntegerGenerator
{
  protected int m_low;
  protected CounterGenerator m_tranInsertGen;

  /**
   * Creates a generator that will return integers uniformly at random starting from <br>
   * low to the last inserted key number. Both ends are inclusive.
   * @param low Lower bound of generated values (inclusive).
   * @param transactionInsertKeyGen Transaction key generator
   */
  public UpdatedUniformIntegerGenerator(int low, CounterGenerator transactionInsertKeyGen)
  {
    m_low = low;
    m_tranInsertGen = transactionInsertKeyGen;
  }

  @Override
  public int nextInt()
  {
    int interval = m_tranInsertGen.lastInt() - m_low + 1;
    int ret = Utils.random().nextInt(interval) + m_low;
    setLastInt(ret);

    return ret;
  }

  @Override
  public double mean()
  {
    return 0;
  }
}
