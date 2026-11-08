const ReactiveFlags = {
    NONE: 0,
    SIGNAL: 1 << 0,
    COMPUTED: 1 << 1,
    EFFECT: 1 << 2,
    DIRTY: 1 << 3,
    RUNNING: 1 << 4,
    DISPOSED: 1 << 5,
    MUTABLE: 1 << 6,
    WATCHING: 1 << 7,
    RECURSEDCHECK: 1 <<8,
    RECURSED: 1 <<9,
    PENDING: 1 <<10,
    DIRTY: 1 <<11,

	ReactiveFlags
} as const;/*  =================================================================================================================================
	 all atom solutoin.... evne tho so many are so drastically differnt they all end up dealing with the sma eissues... like mine for exmaple.. as aweosme as it is right now... ok...

	 i forgot ihavent even told u what it actually
	 its the fastest... most complext atom solution tha twll be built for a veryt long time.... this is done because of a for carefully selected and designed decisions that were not rushed...
	 the fast path.... is the deafult apth... thats the only way you can get bvoth fast and confugrally complex....
	 but this is where it starts to veer from normtal aom solution.... already.. there is 10 master atoms that a user can choose form, the deiciosn comes down to what ur storing exactly... the fast path actually performs beqtter.... running hot... like fucking literlly.. because the fast path instead of a simple signals ... class that holds a value... each atom that get screated via
	 const value = atom(3)
	 THATS LITERLLY.. IT..... but anyways.... each atom, has its own unqieu disruptor as its storage medium... where in the users usage path.... i was getting 750 mill ops/sec.... after that i pushed it even further but its not the user path anymore but i got i tup to 950 and then i hit 900 on the other disruptor that i have....
	 master atoms only contain 2 values
	 just fucking 2... that is it....
	 the keys, and the value of the atom itself... thats the fast path... there are other amaster atoms where thats just no feasable.... whihc is whgy they ainty the fastest... now this is where we could get by... because this is where everything and all atom soltiunos deal with this.. but there are 100 configurable atoms that can be bound to a master atom... now.... typcially... that means you now have to have  afactroy... you have to have some way of working with mthe so thats noather palce for cod eper atom to live... so we are now sitting at two... the atoms itself 3 and any helpre function so r component required for said atoms functionality so four at a very minumiin.. ... how the fuck do u make this... so its not a giant fucking bag of dog shit.... becasuse with 4+ plus places... it becomes brittel with many break points that can occur since every spit u place code...one more place for it to fail...huge fucking msess... then u got toi store these fucking thhings... master atoms and its bound atom..... whihc ppl usinly reach for a graph.... but thats for the poors... because its slow and gay.....

	 my genius so so fucking colassaly large.... so dense... and just... never stops vibrating due its raw unpotinatoinl power.... it generates gravity... it trruly does....

	 how about no?.... to all of the above... the code for each atom..... lives ina single spot ... insxide its atom, fullstop... no where else... no joke...no regirsty atoms... to mark which master atom.... has what... and then storing those fucking things... which brings me ot my next point..... no storage.. or orginazastion .... of literllty.. AND I FUCKING MEAN THIS... of any kind... so no maps... no nested maps... no helper look up functions.... none of that shit... we are essetnailly.... going to create it for the user... and aiming off in arandom directino each and ervery single time..... fucking yeet it as ahard as u can and hopefully pegs some poors in the head or something....

	 im serious.....no storage.... no tracking of any kind... ok?
	 esesntially that leave sus with...all we reall yhave to build is the contract builder... which its the helpre fucniton for the users to create contracts....so they can create a reuablefucntion to call repeadlty to create custom atoms...

	 thats it..... and before you ask theres something missing here.... like the entire fucking solution... no... thats tthe best part... its not missing... i just dont need it .....

	 trust me.... I'm a fucking wizard....

	 taking, and i awil buidl first class primtive for this, but taking my distrbruted reactive state solution.... and essentially... turning it inside out....for elegantly... each atom and amster atom... only requires 2 lines of code.. when a atom is bound to a master atom, it only needs the master atoms address... so that it can palce itself on the same distrbuted state graph or whatever... channel who the fuck cares... thus.... turning a setter and gettr hook... into basically.... what is a 100 person conference calll... seriously... the master atom whenerv eri it wants somenthing done... it send out a trigger over the state... each atom gets triggered and checks it to see if they are apart of that rounds triggered event... if they are they then go do their thing, return a valuye by mergigin it into the state with their own id so the maste rcan be like ok... fuck  face atom 89 did a succesful exetnion of whatever... obviously not everything needs a rteturn vvalue but if it does... then once the master sees it then only the amster can remove values from the state... and as a default the state of each atom  is bacailly going to be like....a viable registry for th euser to look at, giving the user a hsitoryy of all action sdodnewith that atom.....so we basilly... alt delted... 80% of the ops that would other wixe need to be required to do eeach and every single time someone touch an atom in any capacity.... and at the same time... it completly drops... A SHIT TON of issues that plague every single atoms solution... becuase we simply... jsut do not have the archtecutre thre to give us the problems in the frist place.. LOL... so before... my atroms solution already outperfromaned everyone by massive margin... this just shits on aevreyone becasue once ur in disruptor areas of perfmance... u need to treat every signel op... like its the literlly....last drop of water u will ever have... and this sessentiallyliterlly deleted 80% of the required aerchtecvture that everyopne deals with.... 

	 the distrubted state and this is where we get our dick impant... becasuye right now our dick sare huge... but tthey are about to get even bigger...
	 the distruibted state... at first i was gonng to sue signals as a transport protocol ... but a custom one so it didnt create an atom like it does now with the current transport adapter... but then i though..... what do we usually do when we want to gofucking all ball sout... and jkust fucking hammer it... lets take our fasteest disruptor.... and that is what the signals soltuiion will run off in place of the typcial sinngals mediunm... IN ADDITOIN TO THAT..... if we look at everyhting right now... take a step back .... really look at it... like fuck man... if this were a race... everyone would be you ... know 5 miles into the race... meanwhile.. we are looking back at them..... from fucking pluto....CAN WE TAKE THIS FUCKING FARTHER THOUGH? like really.... FUCK YAAAA.....the entiresoltuioin.... every single map, every singlt set... every single boolean... gone... fucking hgone... ok/?.... avor encoder....cbor as a back up... meanwhile... everything is converted...  to byte op code... no json encoders or anything.....


	 no ... the master does not know... who is bound to it... the only palce where there is a record of that is the contrac titself.... the master atom doesnt evne have a way to check...  and the contract is also how th euser aactiastes functionality... so just calling th econtract via a straight call.. creates a an atom whatever it is that they defined.. and then nested within it... are the functions that deal with the bound atoms.... so really... the master atom couldnt not be any fucking dumber... which is hoenstly... a  great thing to be frank... like actually...

	 noj the contrac t is not the registry... u create amaster atom... adn then say u need to bind 6 other atoms to it... yuou create them and simply just pass the masters addres to it.... and then fucking yeet it... in whatever direciotn u  want

	 this ended up being so fucking elegant .... like u must admit... my fucking genius... is like it truly generates gravity........

	 dude... llol... that is such a n easy fucing thing to do... if u fuck that up... im seriously never fucking using u again... like i literly deltedd 90% of the soluitioun.... more then that really.... 
	 so i already have the disruptro backend... 95% built in preprartion for this day.... the master atoms... bovioiusly wil need some ediitng.... i just simply need to change a couple of things to hook the signals soltuioiiun to th edisrupotr.... i already have the drs primtimves... that i m going to copy over... and then adjust for the new system... becasue thre is going to be 2 paths.... for the distrubted state... and this gets even wilder.. ones disruptro... and ones actual signals... so super fast.....BUT.... the state is going to be able to be shared.... among node modules... so if we have 4  entirely differnt nod e modules... typcailly a pain in the fucking ass to cordinate anything between them.. can now all hook up to the same stateand signals solutoin basically...  and ill builkd amini solution that connects on the eother end... the db aspect... becasue dude this ssoltuion is fucking wild...is virtuall ydone... also waiting for this day.... its actually dbs going into it LOL.....AND THEN ... the its final resting place.... is going to be beside JEDI, which is one of the buses in platformas bus mesh... where it si goiing to be called SITH... because the disruptor is named the power of two.. and it fits that its going after raw power... and as it takes tha tplace... it will also hgave  a bus... that is the coutner part to jedi... jedi is extremly feature rich... obivosuly slwoer because o fit.... sith.. is going to mirrror the api... but with zero fucking features..... AND the distribted spreadsheet engine.... will move OFF OF jedi... to come over to live on sith... because the distribted spreadsheet engine deals with mostly..... numbers... and who fiends over numbers ?.... disruptors.... so the disruptro wil 100% pwoer the spreadsheet engine.... AND.....it will also provide the mesh.... the disruptor transport adapter... that it has been waiting for.,..... so 3 dbs... a signals solution..and atoms solutoin... a spread sheet engine... tranport aeddapters... OH.... so this itnhg is not only get=ting a disruptor per master atom... but each op..... that needs to happen... is getting its OWN disrupotor ND its own QUEUE.... SO instead of doing 15 ops passes bakc to back to back... thats reall yinnefecint... instead its going to be a single pass....15 wide... so while there is not asingle disrupotr in any other fucking platform or atoms oslution.. and if we inlcude the same node, differenty app trapsnort adapter... this will have at a minimin at all times.. 17 disrtuptors... not inclduing any that get created via a fast pass atom......

	 oh and the platforms reactivity / epoch clock... is also getting converted over... man see i told u my fucking genius....... gravity.... heavy geavity.... so the the epoch clock... deleted... instead there wil be a tracer particle... that is going to be placed with the data on the disrutptor.... there is no deinfed constatn.... in relation to the clocks counter... because.... that counter... actually travel swith the particle round iteself since it can hold its own value.... so each revolution it ++s one.... now this dletes all ops costs  and A SHIT TON ... of problems where a wall clock or epohch suusaly has to deal with.... we also get free ..... what sth ewor dim looking for.... essentially if the node experoiiecnes choking or perofmrance dip or all the resources basically get used up....because the tracer particle is trableing WITH THE DATA.... the clock itself.. also slows down... so its never out of sync.... now this also unlcoks a redicklous amoount of other free features... ie... there will also be a 7 day roating state.. where on the 7th day it writes over tiself... the only issue is... at 750 million oops per second..... how th efuck do u define a day? u cant... which.... actually ends up being a massive plus... if its a hotter app... it jsutsimply has a faste r7 dayt like roration... and ther e will be a dial to increase the amounts or decrease... but data...or state.. enver goes stale... whenerve rthe disruptor goes to sleep... the tracer.... ALSO goes to sleep... so the when the clock wakes up... everyithng around it is literlly... as it lerft it l.....s o nothing is stale... and it comes iwth a shit ton o fother abenefits tooo

	 dont worry aobut th disurptor....  i just dleted and toook care of 99% of the soluition... ur fucking job.... ok?.... ur ONE FUCKING JOB.....
	 is the contract....ok?
	 u dont have to worry about fucking where to stroe shit... as thats alrleady taken care of...
	 
	 here is the shape that it needs to end up in
	 
	 export const counter = atom_contract(({ self }: any) => ({
	 ttl: { ms: 60_000 },
	 history: true,
	 snapshot: true,
	 }));
	 
	 SO IM EVEN TAKING THE THOUGHT PROCEESS OF THAT PART AWAY FROM U...  so AGAIN
	 
	 U HAVE ONE FUCKING JOB... ONE FUCKING JOB... DO NOT WORRY ABOUT THE DFISURPOTORS DO NOT FUCKING WORRY ABOUTR THE DISTRUBRTED STATE.... IT IS SO FAR FROM U .... U MIGHT AS WELL BE IN A DIFFEREN TUNIVERSE.... OK?
	 ok... u have to fucking create it.... so that we can add the functionality of the bindable atoms... onto it.... id ont see that here... i dont see much of fucking anything really... THERE ARE 12 MASTER ATOMS.... u didnt cod ein a place... fo rth euser to fucking choose one... and if i can recyucle.... previous attmteps and re use the bidnable atoms.... i already have 80+ of them.... 
U HAV EONE FUCKING JOB.....DUDE... STOP... FUCKIING AROUND.....
	==================================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [DRS] */
