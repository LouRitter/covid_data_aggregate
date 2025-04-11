COVID MongoDB Script – Dev Notes

Hey there!

Here's a quick rundown of the decisions I made while building the script, along with some assumptions and small optimizations I threw in to make the process more efficient and reliable.

💾 Part 1: Loading the Dataset

Why use open-uri?

Simple and effective. It lets me treat the remote CSV file like a local file, which made it easy to read and parse using the standard CSV library.

Fields filtering early

Right after parsing, I filtered down the fields to only what was needed. This not only reduces memory usage but also keeps the DB lean and focused for later queries.

Batch inserts

Instead of inserting records one by one (which is really slow with MongoDB), I used .each_slice(5000) to insert in chunks. This greatly speeds things up and avoids hammering the DB with too many small writes.

Data cleaning upfront

Before inserting, I stripped out any empty strings and replaced them with nil. This helps MongoDB treat them as missing rather than empty, which is cleaner for querying later.

📊 Part 2: Analyzing the Data

Using aggregation pipelines

MongoDB’s aggregation framework is super powerful, so I leaned into that heavily. It helps me do things like:

Grab the latest values by sorting and grouping

Calculate cumulative sums

Estimate derived values (like total smokers)

Avoid pulling large datasets into Ruby memory

Total cases logic

I grouped by country and sorted by date descending to get the latest total case number per country, and then summed those. This way we only consider the most up-to-date values.

Smoking stats

The smoking calculation assumes a 50/50 gender split—obviously an oversimplification, but good enough for this context. I picked the latest available numbers for each country when calculating both percentages and estimated smoker counts.

Exclusions
I excluded aggregate regions like "World", "Europe", etc., from specific country-level analyses to avoid skewed results. This seemed reasonable since the exercise focuses on countries.

Console + File output
Every result is both printed and saved to a output.txt file, just to make it easy to submit and review.
