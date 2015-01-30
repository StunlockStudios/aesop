package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import java.util.ArrayList;
import java.util.List;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.AbstractBinLogEventProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public class UpdateEventProcessor extends AbstractBinLogEventProcessor
{
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener)
	{
		UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) event;

		List<Pair<Row>> listOfPairs = updateRowsEvent.getRows();
		List<Row> rowList = new ArrayList<Row>(listOfPairs.size());
		for (Pair<Row> pair : listOfPairs)
		{
			Row row = pair.getAfter();
			rowList.add(row);
		}

		List<AbstractEvent> sourceEvents = map(updateRowsEvent.getTableId(), rowList, listener);
		for (AbstractEvent sourceEvent : sourceEvents)
		{
			listener.getSourceEventConsumer().onEvent(sourceEvent);
		}
	}
}
