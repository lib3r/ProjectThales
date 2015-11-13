Euromaidan, pt.1
===

**Occured: 11/21/2013 - 11/29/2013 <br/>
Data: 2013/11/18 - 2013/12/01**

*Euromaidan was a wave of demonstrations and civil unrest in Ukraine, which began on the night of 21 November 2013 with very large public protests demanding closer European integration*


Discussion
----

This information was gathered from this [wikipedia](http://www.wikiwand.com/en/Timeline_of_the_Euromaidan#/21.E2.80.9329_November_2013) page.

- Euromaidan started in the night of _21 November 2013_ when up to 2,000 protesters gathered at Kiev's Maidan Nezalezhnosti

- Ukrainian government decree to suspend preparations for signing of the Association Agreement on _21 November 2013_, opposition party Batkivshchyna faction leader Arseniy Yatsenyuk called, via Twitter, for protests (which he dubbed as #Euromaidan) on Maidan Nezalezhnosti

- Approximately 2,000 people converged in the evening of _22 November_

- A larger rally took place on _24 November_, when 50,000 to 200,000 people gathered on Kiev's Maidan Nezalezhnosti. The pro-EU demonstrators carrying Ukrainian and EU flags chanted "Ukraine is Europe" and sang the national anthem as they marched toward European Square for the rally. News agencies claimed this to be the largest protest since the Orange Revolution of 2004

Questions to answer
----

1. Can I find evidence of the ukrainian protest inside the GDELT data?
2. Which CAMEO verb codes correspond to this event?
3. What are the important actors in the CAMEO ontology during this time period?
4. How does GDELT define 'important events'? Which measure gives us the best signal?

---

Spark Queries
----
<a name="1"></a>

1.  Number of Events per country, for the top 20 countries

	To start I want to ask some very simple questions that will help us get a better understanding of the data itself.

	The general approach in CAMEO an *event* is an action performed by one entity (Actor1) onto another (Actor2), so it follows,```Actor1-Verb-Actor2```

	```scala
	val actor1 = df.groupBy("Actor1CountryCode").count().orderBy(desc("count"))
	```
	output:

	```
	+-----------------+------+
	|Actor1CountryCode| count|
	+-----------------+------+
	|                 |745417|
	|              USA|328890|
	|              IRN| 45146|
	|              GBR| 44471|
	|              CHN| 37178|
	|              RUS| 32323|
	|              AUS| 24968|
	|              ISR| 24510|
	|              CAN| 21991|
	|              FRA| 21004|
	|              PAK| 19089|
	|              EUR| 15469|
	|              JPN| 15465|
	|              SYR| 15457|
	|              TUR| 15380|
	|              AFG| 15221|
	|              DEU| 13965|
	|              PHL| 13895|
	|              IND| 13579|
	|              AFR| 12981|
	+-----------------+------+
	only showing top 20 rows
	```

	- This shows that events in the GDELT set are biased to western news coverage. Let's try to get a better signal with this later.

2. filter data to only look at events in the form ``RUS<verb>RUS`` or ``UKR<verb>UKR``.

	```scala
	val rus_ukr = df.filter("(Actor1CountryCode = 'UKR' and Actor2CountryCode = 'RUS') or (Actor1CountryCode = 'RUS' and Actor2CountryCode = 'UKR')")
	```

	Now I will apply the transformation in [1](#1) above to look at the relationship between actor1 and actor2

	- Actor 1:

		```
		+-----------------+-----+
		|Actor1CountryCode|count|
		+-----------------+-----+
		|              UKR| 1385|
		|              RUS| 1361|
		+-----------------+-----+
		```

	- Actor 2:

		```
		+-----------------+-----+
		|Actor2CountryCode|count|
		+-----------------+-----+
		|              RUS| 1385|
		|              UKR| 1361|
		+-----------------+-----+
		```

	It is very interesting to note that there is little difference between actor1 and actor2 here. Given the time period, I was expecting Actor2 to be more Ukraine than anything else, given that Russia was the aggressor.

3. Lets look at the distribution of CAMEO verbs for these actors

	- The entire CAMEO event taxonomy is ultimately organized under four primary classifications:
		- Verbal Cooperation: ``1``
		- Material Cooperation: ``2``
		- Verbal Conflict: ``3``
		- Material Conflict: ``4``

		This field specifies this primary classification for the event type, allowing analysis at the highest level of aggregation.

		```
		+---------------------------+---+---+---+---+
		|Actor1CountryCode_QuadClass|  1|  2|  3|  4|
		+---------------------------+---+---+---+---+
		|                        UKR|932|148|228| 77|
		|                        RUS|735|125|364|137|
		+---------------------------+---+---+---+---+
		```

		A few things I notice immediately, Ukraine has more events corresponding to cooperation where (more importantly) Russia has a much larger proportion of events that show conflict.

	- next, we'll consider the top level CAMEO verb ontology

		```scala
		rus_ukr.stat.crosstab("Actor1CountryCode", "EventRootCode").show()
		```

		```
		+-------------------------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
		|Actor1CountryCode_EventRootCode| 01| 02| 03| 04| 05| 06| 08| 09| 10| 11| 12| 13| 14| 16| 17| 19| 07| 18|
		+-------------------------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
		|                            UKR|158|121|118|321|214| 80| 60|  8| 19|108| 63| 11| 27| 23| 26| 28|  0|  0|
		|                            RUS| 92| 81| 81|304|177| 66| 31|  6|121| 83| 62| 78| 20| 36| 55| 41| 22|  5|
		+-------------------------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
		```

		We have the most events that fall under category ``04 - Consult`` more than many others.

		In terms of the proportion of events in all categories (bypassing categories where either nation-state had a 0 count) ``13 - Threaten`` is also an interesting result. Let's dig deeper and see if there is anything else we can say.

	- Event Subcode Analysis

		First I will filter per ``EventRootCode`` to see is there are any patterns in the more detailed ``EventCode``

		```scala
		rus_ukr.filter("EventRootCode = '01'").stat.crosstab("Actor1CountryCode", "EventCode").show()
		```

		**Results**

		```
		MAKE PUBLIC STATEMENT
		+---------------------------+---+---+---+---+---+---+---+---+---+
		|Actor1CountryCode_EventCode|010|011|012|013|014|015|016|017|019|
		+---------------------------+---+---+---+---+---+---+---+---+---+
		|                        UKR| 54|  2| 40| 40|  8|  1|  1| 12|  0|
		|                        RUS| 43|  1| 18|  4| 14|  2|  7|  0|  3|
		+---------------------------+---+---+---+---+---+---+---+---+---+
		```

		```
		APPEAL
		+---------------------------+---+---+---+----+---+---+---+----+
		|Actor1CountryCode_EventCode|020|023|026|0214|022|025|027|0241|
		+---------------------------+---+---+---+----+---+---+---+----+
		|                        UKR|107|  1| 12|   0|  0|  0|  0|   1|
		|                        RUS| 62|  0|  5|   1|  4|  1|  8|   0|
		+---------------------------+---+---+---+----+---+---+---+----+
		```

		```
		EXPRESS INTENT TO COOPERATE
		+---------------------------+----+----+---+---+---+---+---+----+----+
		|Actor1CountryCode_EventCode|0311|0331|030|033|035|036|032|0353|0356|
		+---------------------------+----+----+---+---+---+---+---+----+----+
		|                        UKR|   6|   4| 47|  1|  7| 50|  0|   2|   1|
		|                        RUS|   4|   2| 37|  1| 11| 23|  3|   0|   0|
		+---------------------------+----+----+---+---+---+---+---+----+----+
		```

		```
		CONSULT
		+---------------------------+---+---+---+---+---+
		|Actor1CountryCode_EventCode|040|042|043|045|046|
		+---------------------------+---+---+---+---+---+
		|                        UKR| 66| 73| 39|  3|140|
		|                        RUS| 54| 43| 77|  0|130|
		+---------------------------+---+---+---+---+---+
		```

		```
		ENGAGE IN DIPLOMATIC COOPERATION
		+---------------------------+---+---+---+
		|Actor1CountryCode_EventCode|050|051|057|
		+---------------------------+---+---+---+
		|                        UKR| 76| 37|101|
		|                        RUS| 42| 49| 86|
		+---------------------------+---+---+---+
		```

		```
		ENGAGE IN MATERIAL COOPERATION
		+---------------------------+---+---+---+
		|Actor1CountryCode_EventCode|060|061|062|
		+---------------------------+---+---+---+
		|                        UKR| 18| 59|  3|
		|                        RUS| 12| 52|  2|
		+---------------------------+---+---+---+
		```

		```
		PROVIDE AID
		+---------------------------+---+---+---+
		|Actor1CountryCode_EventCode|070|071|073|
		+---------------------------+---+---+---+
		|                        RUS|  8|  9|  5|
		+---------------------------+---+---+---+
		```

		```
		YIELD
		+---------------------------+----+---+---+---+---+----+
		|Actor1CountryCode_EventCode|0874|080|081|084|085|0831|
		+---------------------------+----+---+---+---+---+----+
		|                        UKR|  19| 21|  7|  7|  4|   2|
		|                        RUS|   1| 10| 13|  7|  0|   0|
		+---------------------------+----+---+---+---+---+----+
		```

		```
		INVESTIGATE
		+---------------------------+---+
		|Actor1CountryCode_EventCode|090|
		+---------------------------+---+
		|                        UKR|  8|
		|                        RUS|  6|
		+---------------------------+---+
		```

		```
		DEMAND
		+---------------------------+----+----+----+---+----+
		|Actor1CountryCode_EventCode|1041|1043|1053|100|1044|
		+---------------------------+----+----+----+---+----+
		|                        UKR|   0|   5|   0| 12|   2|
		|                        RUS|   3|   2|   1|115|   0|
		+---------------------------+----+----+----+---+----+
		```

		```
		DISAPPROVE
		+---------------------------+---+---+---+---+----+
		|Actor1CountryCode_EventCode|110|111|112|114|1123|
		+---------------------------+---+---+---+---+----+
		|                        UKR| 29| 30| 37| 10|   2|
		|                        RUS| 26| 30| 15| 12|   0|
		+---------------------------+---+---+---+---+----+
		```

		```
		REJECT
		+---------------------------+---+---+---+---+----+----+
		|Actor1CountryCode_EventCode|120|127|128|129|1211|1243|
		+---------------------------+---+---+---+---+----+----+
		|                        UKR| 51|  1|  5|  4|   0|   2|
		|                        RUS| 50|  3|  2|  3|   4|   0|
		+---------------------------+---+---+---+---+----+----+
		```

		```
		THREATEN
		+---------------------------+----+---+---+---+
		|Actor1CountryCode_EventCode|1312|130|134|138|
		+---------------------------+----+---+---+---+
		|                        UKR|   0| 11|  0|  0|
		|                        RUS|   2| 63|  1| 12|
		+---------------------------+----+---+---+---+
		```

		```
		PROTEST
		+---------------------------+---+---+---+---+
		|Actor1CountryCode_EventCode|141|142|140|145|
		+---------------------------+---+---+---+---+
		|                        UKR| 21|  6|  0|  0|
		|                        RUS| 15|  0|  2|  3|
		+---------------------------+---+---+---+---+
		```

		```
		REDUCE RELATIONS
		+---------------------------+---+---+---+----+---+---+---+
		|Actor1CountryCode_EventCode|160|161|164|1621|162|163|166|
		+---------------------------+---+---+---+----+---+---+---+
		|                        UKR|  7|  5| 11|   0|  0|  0|  0|
		|                        RUS|  4|  5|  1|   3| 12|  9|  2|
		+---------------------------+---+---+---+----+---+---+---+
		```

		```
		COERCE
		+---------------------------+----+----+---+---+---+
		|Actor1CountryCode_EventCode|1721|1722|172|173|175|
		+---------------------------+----+----+---+---+---+
		|                        UKR|   1|   0|  6| 16|  3|
		|                        RUS|   0|   1| 21| 33|  0|
		+---------------------------+----+----+---+---+---+
		```

		```
		ASSAULT
		+---------------------------+---+---+
		|Actor1CountryCode_EventCode|181|182|
		+---------------------------+---+---+
		|                        RUS|  2|  3|
		+---------------------------+---+---+
		```

		```		
		FIGHT
		+---------------------------+---+---+---+---+
		|Actor1CountryCode_EventCode|190|193|191|192|
		+---------------------------+---+---+---+---+
		|                        UKR| 27|  1|  0|  0|
		|                        RUS| 31|  3|  5|  2|
		+---------------------------+---+---+---+---+
		```
	---
