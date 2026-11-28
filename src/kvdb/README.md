
<div align="left" style="position: relative;">
<img src="./icons/darthvader.svg" align="right" width="30%" style="margin: -20px 0 0 20px;">

<h3  style="font-size:26px;">Welcome... To the database dark side. <span style="font-size:16px;">Where raw power, is the only thing we crave and strive for.</span></h3>

<p align="left"><em>The most opinionated platform... ever.</em></p>
<p align="left"><em>Trust me... I'm a fucking wizard... 🧙</em></p>
<p align="left">Platforms stats:</p>
<p align="left">
<img src="https://img.shields.io/badge/LINES OF CODE- 10,300,000-95ED27?style=flat">
<img src="https://img.shields.io/badge/FEATURES- UNKNOWN-95ED27?style=flat">
<img src="https://img.shields.io/badge/REACTS RULES ON HOOKS- 0-95ED27?style=flat">
<img src="https://img.shields.io/badge/DISRUPTORS- 18+-95ED27?style=flat">
<img src="https://img.shields.io/badge/MESSAGE QUEUES- 5-95ED27?style=flat">
</p>
<p align="left">VSCode ext:</p>
<p align="left">
<img src="https://img.shields.io/badge/THEMES- 251-95ED27?style=flat">
<img src="https://img.shields.io/badge/SIDEBARS- 160+-95ED27?style=flat">
<img src="https://img.shields.io/badge/PRODUCT ICONS- 651-95ED27?style=flat">
<img src="https://img.shields.io/badge/FILE ICONS- 454-95ED27?style=flat">
<img src="https://img.shields.io/badge/VSCode Extensions- 82+-95ED27?style=flat">
<img src="https://img.shields.io/badge/FEATURES- SHIT TONNE-95ED27?style=flat">
</p>
<p align="left">Small but powerful repos worth checking out:</p>
<p align="left">

- [![🧙 Alias Types](https://img.shields.io/badge/Alias%20Types-000000.svg?style=flat&logo=typescript&logoColor=white)](https://github.com/the-w1zard/alias_types) <a href="https://www.npmjs.com/package/@the-w1zard/alias_types"><img src="./icons/npm.png" alt="alias_types" width="20" height="20"></a>
- [![🧙 The Cloud](https://img.shields.io/badge/%F0%9F%A7%99%20App%20Store-000000.svg?style=flat)](https://github.com/the-w1zard/app_store)  <a href="https://www.npmjs.com/package/@the-w1zard/store"><img src="./icons/npm.png" alt="app_store" width="20" height="20"></a>
- [![🧙 The Cloud](https://img.shields.io/badge/%F0%9F%A7%99%20DX-95ED27.svg?style=flat)](https://github.com/the-w1zard/dx) <a href="https://www.npmjs.com/package/@the-w1zard/dx"><img src="./icons/npm.png" alt="dx" width="20" height="20"></a>
- [![🧙 The Cloud](https://img.shields.io/badge/%F0%9F%A7%99%20Temporal%20Shift-77AA99.svg?style=flat)](https://github.com/the-w1zard/temporal_shift) <a href="https://www.npmjs.com/package/@the-w1zard/temporal_shift"><img src="./icons/npm.png" alt="temporal_shift" width="20" height="20"></a>
- [![🧙 The Cloud](https://img.shields.io/badge/%F0%9F%A7%99%20The%20Cloud-02303A.svg?style=flat)](https://github.com/the-w1zard/the_cloud)  <a href="https://www.npmjs.com/package/@the-w1zard/the-cloud"><img src="./icons/npm.png" alt="app_store" width="20" height="20"></a>
- [![🧙 SINGULARITY](https://img.shields.io/badge/%F0%9F%A7%99%20SINGULARITY-02303A.svg?style=flat)](https://github.com/the-w1zard)  
- [![🧙 Sometimes,%20my%20genius...%20It%20generates%20gravity.](https://img.shields.io/badge/%F0%9F%A7%99%20Sometimes,%20my%20genius...%20It%20generates%20gravity.-4285F4.svg?style=flat)](https://github.com/the-w1zard)  
- [![🧙 The Wizard](https://img.shields.io/badge/%F0%9F%A7%99%20Coming%20Soon:%20The%20Wizard%20Ed.-7F52FF.svg?style=flat)](https://github.com/the-w1zard)  
- [![🧙 Hadron Particle Disruptor](https://img.shields.io/badge/%F0%9F%A7%99%20Coming%20Soon:%20Hadron%20Particle%20Disruptor-23ED8B00.svg?style=flat)](https://github.com/the-w1zard)  
</p>
<p align="left">The goal? To remove all friction, from everything.</p>

</div>
<br clear="right">

> [!NOTE]
>
> Typically, I let ai write the most of my readmes... this is one of those times where, that can't really be done. This is not your grandpas dusty old db, actually, this is not a db you will find else where, really.
>
> The decisions that went into this... are far from conventional, for example the user api such as get, set, update, delete and so on... are all sync, ALL OF THEM. Both server AND client side, as this is a client bridged WAL KVDB. Its even strapped with a custom file writing solution, so you won't find mysql here as there is no deps, other than typescript and node of course.
>
> Which means, it has REALLY high performance numbers. The closest thing I could find that came as close as it did, but it was still a far distance away, was a db solution... that employed a server farm. 
>
> There is no server farm included with this solution, yet still reaches A LOT higher throughput. So much throughput that, during the making of it I was experiencing a performance drop and it was a massive drop. Naturally, I would review and fix the code... open the bench, run it till it produced the first metric which was one sec into the test, and stopped it. Eventually I got it up the to the number I was expecting.
>
> Later that day, went to push to git...
>
> Git: 'Push rejected due to file size over 100mb'
>
> `what...` *deletes 4 files purely based on assumption and continues working on something else...*
>
> Git: 'Push rejected due to file size over 100mb'
>
> *sighs and mumbles* `I fucking hate you so much...`
>
> *finally, actually looks for the problem* `The db file?... What, how... I haven't even benched or tested it yet... how does it have... GBS!@... wtf... `
>
> Well, after doing some maths... haha, apparently when dealing with THIS much throughput... it doesn't take long before you start generating, gb's and gb's of stored data.
>
> This worked SO well, despite the fact that... all my other dbs are finished, each of them will get the same rework done to them as well, which will include:
> - vdb
> - gdb
> - tdb
> - uhmm, I apparently have too many dbs... RDB, thats it
>
> Unfortunately, this type of db DOES NOT work so well as an adapter, which is why I cannot add it to the current db solutions. These will be available shortly.
>
> Some dev: *barks* `No fucking way... nope... can't be done. Not with a wal solution that has binary encoding and everything you claim it has... won't accept it because reality is just that, reality`
>
> ... Haha, ok... bench away my dude. Genuinely, when someone said that, I just died laughing in front of them. Which... just pissed them off even more.
>
> ... hahaha... It literally just happened again when I asked someone to read this for me. `its not that its not reality, its arithmetic...` hhahaah... I literally just responded with...
>
> `Trust me... I'm a fucking wizard... 🧙` lmfao... got fucking tears in my eyes. At the time of writing benches were hitting between 730-890 million. I know that's a large delta, so below I'll be posting the metrics over a wide area of metrics. 
>
> Yes, I know the bench metrics aren't up yet... they will be. For this solution, I am only doing it once and at MAX 2 runs. So even if those runs produce a lower number than what I have seen previously, I'm not doing any more. Think about it, at almost a billion ops per second, how long does it take to generate a single gb? I run tests for at least 12 secs, and they go upwards to a minute... sometimes a minute +.
>
> With a 8 byte load, that equates to 8 gb/s. With a 16 byte load, 16 gb/s. 32 bytes, 32 gb/s... at 32 bytes running my smallest bench, ONCE... I will have generated 384 gbs over 12 secs. If I don't like the first number, then its 768 gbs. Running it for a minute would produce 1,920 GB, and a minute is still shorter than any of my signal benches, where I test against ~20 other solutions.














