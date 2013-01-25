package com.netflix.astyanax.entitystore;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * same field annotated by both @Id and @Column
 * @author stevenwu
 *
 */
@Entity
public class DoubleIdColumnEntity {

	@Id
	@Column(name="id")
	private String id;
	
	@Column(name="num")
	private int num;
	
	@Column(name="str")
	private String str;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public String getStr() {
		return str;
	}

	public void setStr(String str) {
		this.str = str;
	}

	@Override
	public String toString() {
		return String.format("DoubleIdColumnEntity(id = %s, num = %d, str = %s", 
				id, num, str);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if (obj == null)
			return false;

		if (getClass() != obj.getClass())
			return false;

		DoubleIdColumnEntity other = (DoubleIdColumnEntity) obj;
		if(id.equals(other.id) &&
				num == other.num &&
				str.equals(other.str)
				)
			return true;
		else
			return false;
	}
}
