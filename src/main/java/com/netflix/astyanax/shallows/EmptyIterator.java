package com.netflix.astyanax.shallows;

import java.util.Iterator;
import java.util.NoSuchElementException;

@SuppressWarnings("rawtypes")
public class EmptyIterator implements Iterator {

	public static final Iterator Instance = new EmptyIterator();
	
	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public Object next() {
		throw new NoSuchElementException();
	}

	@Override
	public void remove() {
		throw new NoSuchElementException();
	}

}
