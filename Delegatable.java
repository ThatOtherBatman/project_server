package com.teza.common.tardis;

/**
 * User: tom
 * Date: 1/5/17
 * Time: 5:41 PM
 */

/**
 * specifies that a class has a delegate
 * @param <T> the type of the delegate
 */
public interface Delegatable<T>
{
    void setDelegate(T delegate);
}
