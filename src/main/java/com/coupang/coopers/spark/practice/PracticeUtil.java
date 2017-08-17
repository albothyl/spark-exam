package com.coupang.coopers.spark.practice;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class PracticeUtil {

	private static Map<Long, String> SRC_MAP = Maps.newHashMap();
	private static Map<Long, String> SPEC_MAP = Maps.newHashMap();

	public static String findSrc(final Long src) {
		return SRC_MAP.get(src);
	}

	public static String findSpec(final Long spec) {
		return SPEC_MAP.get(spec);
	}

	public static List<ClickEventDTO> getClickEventDTOList() {
		List<ClickEventDTO> clickEventDTOList = Lists.newArrayList();
		clickEventDTOList.add(ClickEventDTO.create(1043001L, 10304103L));
		clickEventDTOList.add(ClickEventDTO.create(1043001L, 10304103L));
		clickEventDTOList.add(ClickEventDTO.create(1043016L, 10304924L));
		clickEventDTOList.add(ClickEventDTO.create(1083010L, 10599004L));
		clickEventDTOList.add(ClickEventDTO.create(1083010L, 10599004L));
		clickEventDTOList.add(ClickEventDTO.create(1083010L, 10599004L));
		clickEventDTOList.add(ClickEventDTO.create(1083010L, 10599004L));
		clickEventDTOList.add(ClickEventDTO.create(2059000L, 10101507L));
		clickEventDTOList.add(ClickEventDTO.create(2153000L, 10909999L));
		clickEventDTOList.add(ClickEventDTO.create(2153000L, 10909999L));
		clickEventDTOList.add(ClickEventDTO.create(2153000L, 10909999L));
		clickEventDTOList.add(ClickEventDTO.create(2153000L, 10909999L));
		clickEventDTOList.add(ClickEventDTO.create(2153000L, 10909999L));

		return clickEventDTOList;
	}

	static {
		SRC_MAP.put(1043001L, "Paid.SA.MW.Naver");
		SRC_MAP.put(1043016L, "Paid.SA.MW.Google");
		SRC_MAP.put(1083010L, "Paid.SB.MW.Adbay");
		SRC_MAP.put(2059000L, "NonPaid.Email.Others.Coupang");
		SRC_MAP.put(2153000L, "NonPaid.Banner.MW.Coupang");

		SPEC_MAP.put(10304103L, "v10.Potal.Search");
		SPEC_MAP.put(10304924L, "v10.Portal.Search");
		SPEC_MAP.put(10599004L, "v10.Network.Others");
		SPEC_MAP.put(10101507L, "v10.Email.Daily");
		SPEC_MAP.put(10909999L, "v10.Others.Mobileweb_quarter");
	}
}
