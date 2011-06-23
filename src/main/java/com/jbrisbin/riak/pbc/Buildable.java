package com.jbrisbin.riak.pbc;

import com.google.protobuf.Message;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public interface Buildable<T extends Message> {

	T build();

}
