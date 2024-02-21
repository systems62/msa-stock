package com.predic8.stock.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.predic8.stock.model.Stock;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ShopListener {
	private final ObjectMapper mapper;
	private final NullAwareBeanUtilsBean beanUtils;
	private final Map<String, Stock> stocks;

	public ShopListener(ObjectMapper mapper, NullAwareBeanUtilsBean beanUtils, Map<String, Stock> stocks) {
		this.mapper = mapper;
		this.stocks = stocks;
		this.beanUtils = beanUtils;
	}

	@KafkaListener(topics = "shop")
	public void listen(Operation op) throws Exception {
		System.out.println("op = " + op);
		
		if(op.getAction().equals("upsert") && op.getBo().equals("article")) {
			Stock stock = mapper.treeToValue(op.getObject(), Stock.class);		
			stocks.put(stock.getUuid(), stock);
		}
	}
}