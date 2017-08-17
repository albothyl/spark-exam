package com.coupang.coopers.spark.practice;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor(staticName = "create")
public class ClickEventDTO implements Serializable {
	private Long src;
	private Long spec;
}
