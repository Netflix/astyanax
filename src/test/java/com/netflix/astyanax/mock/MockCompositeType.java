/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.mock;

import com.netflix.astyanax.annotations.Component;

public class MockCompositeType {
	@Component
	private String stringPart;
	
	@Component
	private Integer intPart;
	
	@Component
	private Integer intPart2;
	
	public MockCompositeType() {
		
	}
	
	public MockCompositeType(String part1, Integer part2, Integer part3) {
		this.stringPart = part1;
		this.intPart = part2;
		this.intPart2 = part3;
	}
	
	public void setStringPart(String part) {
		this.stringPart = part;
	}
	
	public String getStringPart() {
		return this.stringPart;
	}
	
	public void setIntPart1(int value) {
		this.intPart = value;
	}
	
	public int getIntPart1() {
		return this.intPart;
	}

	public void setIntPart2(int value) {
		this.intPart2 = value;
	}
	
	public int getIntPart2() {
		return this.intPart2;
	}
	
	public String toString() {
		return new StringBuilder().append("MockCompositeType[")
			.append(stringPart).append(',')
			.append(intPart).append(',')
			.append(intPart2).append(']').toString();
	}
}
