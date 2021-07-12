package com.exactpro.th2.crawler.util.impl;

import com.exactpro.th2.crawler.util.CrawlerTime;

import java.time.Instant;

public class CrawlerTimeImpl implements CrawlerTime {
    @Override
    public Instant now() {
        return Instant.now();
    }
}
