package com.yonyou.cloud.exceptions;

/**
 * 非法的DistinctID
 * @author daniell
 *
 */
public class InvalidArgumentException extends Exception {

  public InvalidArgumentException(String message) {
    super(message);
  }

  public InvalidArgumentException(Throwable error) {
    super(error);
  }

}
