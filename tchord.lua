-------------------------------------------------------------------------------
-- modules
-------------------------------------------------------------------------------

require"splay.base"
rpc = require"splay.rpc"
rpc.l_o.level=1
misc = require "splay.misc"
crypto = require "crypto"

-- addition to allow local run
PARAMS={}
local cmd_line_args=nil
if not job then --outside the sandbox
	if #arg < 2 then  
		print("lua ", arg[0], " my_position nb_nodes")  
		os.exit()  
	else  		
		local pos, total = tonumber(arg[1]), tonumber(arg[2])  
		local utils = require("splay.utils")
		job = utils.generate_job(pos, total, 20001) 
		cmd_line_args=arg[3]	
	end
end

if arg~=nil then
	if cmd_line_args==nil then cmd_line_args=arg[1] end
	if cmd_line_args~=nil and cmd_line_args~="" then
		print("ARGS: ",cmd_line_args)	
		for _,v in pairs(misc.split(cmd_line_args,":")) do
			local t=misc.split(v,"=")
			PARAMS[t[1]]=t[2]
		end
	end
end


rpc.server(job.me.port)
-------------------------------------------------------------------------------
-- current node
-------------------------------------------------------------------------------
--current nodes's id
me = {}
me.peer = job.me
M = 32
function compute_hash(o)
	return tonumber(string.sub(crypto.evp.new("sha1"):digest(o), 1, M/4), 16)
end

--current node's age
me.age = 0

me.id = compute_hash(table.concat({tostring(job.me.ip),":",tostring(job.me.port)}))

-------------------------------------------------------------------------------
-- parameters
-------------------------------------------------------------------------------

--T-CHORD params
GOSSIP_TIME = tonumber(PARAMS["GOSSIP_TIME"]) or 5
TCHORD_EXPONENT = 7
--size of leaf set
TCHORD_LEAF = tonumber(PARAMS["TCHORD_LEAF"]) or 3
-- size of a random sample from pss to be used in TCHORD exchanges
TCHORD_RANDOM = tonumber(PARAMS["TCHORD_RANDOM"]) or 2
TCHORD_HB_TIMEOUT = tonumber(PARAMS["TCHORD_HB_TIMEOUT"]) or 300
TCHORD_CONVERGE = PARAMS["TCHORD_CONVERGE"] or true

--PSS params
PSS_VIEW_SIZE =tonumber(PARAMS["PSS_VIEW_SIZE"]) or 10
PSS_SHUFFLE_SIZE =  tonumber(PARAMS["PSS_SHUFFLE_SIZE"]) or math.floor(PSS_VIEW_SIZE / 2 + 0.5)
PSS_SHUFFLE_PERIOD = tonumber(PARAMS["PSS_SHUFFLE_PERIOD"]) or 10


-- ############################################################################
-- 	Peer Sampling Service
-- ############################################################################

PSS = {

	view = {},
	view_copy = {},
	c = PSS_VIEW_SIZE,
	exch = PSS_SHUFFLE_SIZE,
	S = math.floor(PSS_VIEW_SIZE/ 2 + 0.5),
	H = 0,
	SEL = "rand", -- could also be "tail"
	view_copy_lock = events.lock(),
	
	-- utilities
	print_table = function(t)
		print("[ (size "..#t..")")
		for i=1,#t do
			print("  "..i.." : ".."["..t[i].peer.ip..":"..t[i].peer.port.."] - age: "..t[i].age.." - id: "..t[i].id)
		end
		print("]")
	end,
	
	set_of_peers_to_string = function(v)
		ret = ""; for i=1,#v do	ret = ret..v[i].id.." "	end
		return ret
	end,
	
	print_set_of_peers = function(v,message)	
		if message then log:print(message) end
		log:print(PSS.set_of_peers_to_string(v))
	end,
	
	print_view = function(message)
		if message then log:print(message) end
		log:print("PSS VIEW_CONTENT "..job.position.." "..PSS.set_of_peers_to_string(PSS.view))
	end,
	
	-- peer sampling functions
	
	pss_selectPartner= function()
		if PSS.SEL == "rand" then return math.random(#PSS.view) end
		if PSS.SEL == "tail" then
			local ret_ind = -1 ; local ret_age = -1
			for i,p in pairs(PSS.view) do
				if (p.age > ret_age) then ret_ind = i;ret_age=p.age end
			end
			assert (not (ret_ind == -1))
			return ret_ind
		end
	end,
	
	same_peer_but_different_ages = function(a,b)
		return a.peer.ip == b.peer.ip and a.peer.port == b.peer.port
	end,

	same_peer = function(a,b)
		return PSS.same_peer_but_different_ages(a,b) and a.age == b.age
	end,
	
	pss_selectToSend = function()
		-- create a new return buffer
		local toSend = {}
		-- append the local node view age 0
		table.insert(toSend,{peer={ip=job.me.ip,port=job.me.port},age=0,id=me.id})
		-- shuffle view
		PSS.view = misc.shuffle(PSS.view)
		-- move oldest H items to the end of the view
		--- 1. copy the view
		local tmp_view = misc.dup(PSS.view)
		--- 2. sort the items based on the age
		table.sort(tmp_view,function(a,b) return a.age < b.age end)
		--- 3. get the H largest aged elements from the tmp_view, remove them from the view 
		---    (we assume there are no duplicates in the view at this point!)
		---    and put them at the end of the view
		for i=(#tmp_view-PSS.H+1),#tmp_view do
			local ind = -1
			for j=1,#PSS.view do
				if PSS.same_peer(tmp_view[i],PSS.view[j]) then ind=j; break end
			end
			assert (not (ind == -1))
			elem = table.remove(PSS.view,ind)
			PSS.view[#PSS.view+1] = elem
		end
	
		-- append the first exch-1 elements of view to toSend
		for i=1,(PSS.exch-1) do
			toSend[#toSend+1]=PSS.view[i]
		end		
	
		return toSend
	end,
	
	pss_selectToKeep = function(received)
		local selectToKeepStart= misc.time()	
		-- concatenate the view and the received set of view items
		for j=1,#received do PSS.view[#PSS.view+1] = received[j] end
		
		-- remove duplicates from view
		-- note that we can't rely on sorting the table as we need its order later
		local i = 1	
		while i < #PSS.view-1 do
			for j=i+1,#PSS.view do
				if PSS.same_peer_but_different_ages(PSS.view[i],PSS.view[j]) then
					-- delete the oldest
					if PSS.view[i].age < PSS.view[j].age then 
						table.remove(PSS.view,j)
					else
						table.remove(PSS.view,i)
					end
					i = i - 1 -- we need to retest for i in case there is one more duplicate
					break
				end
			end
			i = i + 1
		end
	
		-- remove the min(H,#view-c) oldest items from view
		local o = math.min(PSS.H,#PSS.view-PSS.c)
		while o > 0 do
			-- brute force -- remove the oldest
			local oldest_index = -1
			local oldest_age = -1
			for i=1,#PSS.view do 
				if oldest_age < PSS.view[i].age then
					oldest_age = PSS.view[i].age
					oldest_index = i
				end
			end
			assert (not (oldest_index == -1))
			table.remove(PSS.view,oldest_index)
			o = o - 1
		end
		
		-- remove the min(S,#view-c) head items from view
		o = math.min(PSS.S,#PSS.view-PSS.c)
		while o > 0 do
			table.remove(PSS.view,1) -- not optimal
			o = o - 1
		end
		
		-- in the case there still are too many peers in the view, remove at random
		while #PSS.view > PSS.c do table.remove(PSS.view,math.random(#PSS.view)) end
	
		assert (#PSS.view <= PSS.c)
		--log:print("PSS_SELECT_TO_KEEP ", ( misc.time() - selectToKeepStart ) )		
	end,
	
	ongoing_at_rpc=false,
	
	pss_passive_thread = function(from,buffer)
		if PSS.ongoing_at_rpc then
			return false
		end
		--PSS.print_view("passive_thread ("..job.position.."): entering")
		--PSS.print_set_of_peers(buffer,"passive_thread ("..job.position.."): received from "..from)
		local ret = PSS.pss_selectToSend()
		PSS.pss_selectToKeep(buffer)
		--PSS.print_view("passive_thread ("..job.position.."): after selectToKeep")
		return ret
	end,
	
	pss_send_at_rpc = function(peer,pos,buf)
		local ok, r = rpc.acall(peer,{"PSS.pss_passive_thread", pos, buf},PSS_SHUFFLE_PERIOD/2)
		return ok,r
	end,
	
	pss_active_thread = function()
		PSS.ongoing_at_rpc=true
		-- select a partner
		local exchange_aborted=true
		local exchange_retry=2
		for i=1,exchange_retry do --up to 2 attemps per round, re-do in case of conflict 
			partner_ind = PSS.pss_selectPartner()
			partner = PSS.view[partner_ind]
			-- remove the partner from the view
			table.remove(PSS.view,partner_ind)
			-- select what to send to the partner
			buffer = PSS.pss_selectToSend()
			--PSS.print_set_of_peers(buffer,"active_thread ("..job.position.."): sending to "..partner.id)

			-- send to the partner
			local rpcStart=misc.time()
			local ok, r = PSS.pss_send_at_rpc(partner.peer,job.position, buffer) -- rpc.acall(partner.peer,{"PSS.pss_passive_thread", job.position, buffer},PSS_SHUFFLE_PERIOD/2)
			--log:print("PSS.pss_passive_thread.RPC ",  misc.time() - rpcStart  )

			if ok then
				-- select what to keep etc.
				local received = r[1]
				if received==false then
					log:print("PSS received false due to ongoing RPC, will try again in a short while")
					events.sleep(math.random())	
					--the call was aborted due to pending RPC at peer's node
				else
					exchange_aborted=false 
					--PSS.print_set_of_peers(received,"active_thread ("..job.position.."): received from "..partner.id)
					PSS.pss_selectToKeep(received)
					--PSS.print_view("active_thread ("..job.position.."): after selectToKeep")
				end
			else
				-- peer not replying? remove it from view!
				log:print("on peer ("..job.position..") peer "..partner.id.." did not respond -- removing it from the view")
				log:warning("PSS.pss_passive_thread RPC error:", r)
				table.remove(PSS.view,partner_ind)
			end		
			if exchange_aborted==false then break end
		end
	
		PSS.view_copy_lock:lock()
		local viewCopyLock = misc.time()
		PSS.view_copy = misc.dup(PSS.view)
		--log:print("PSS_VIEW_COPY_LOCK_HELD ", ( misc.time() - viewCopyLock ) )
		PSS.view_copy_lock:unlock()
		for _,v in ipairs(PSS.view) do
				v.age = v.age+1
		end
		-- now, allow to have an incoming passive thread request
		PSS.ongoing_at_rpc=false
	end,
	
	-- API
	pss_getPeer = function()
		PSS.view_copy_lock:lock()
		local getPeerLockHeldStart = misc.time()
		
		local peer = PSS.view_copy[math.random(#PSS.view_copy)] 

		--log:print("PSS_GET_PEER_LOCK_HELD_VIEW_COPY ", ( misc.time() - getPeerLockHeldStart ) )
		PSS.view_copy_lock:unlock()

		return peer
	end,

	pss_init = function()
		-- ideally, would perform a random walk on an existing overlay
		-- but here we emerge from the void, so let's use the Splay provided peers.
		-- Ages are taken randomly in [0..c] but could be 0 as well.
		local indexes = {}
		for i=1,#job.nodes do indexes[#indexes+1]=i end
		table.remove(indexes,job.position) --remove myself
		local selected_indexes = misc.random_pick(indexes,math.min(PSS.c,#indexes))	
		for _,v in ipairs(selected_indexes) do
				local a_peer = job.nodes[v]
				local hashed_index = compute_hash(tostring(a_peer.ip) ..":"..tostring(a_peer.port))
		 		PSS.view[#PSS.view+1] = 
				{peer=a_peer,age=math.random(PSS.c),id=hashed_index}
		end
		PSS.view_copy = misc.dup(PSS.view)
		assert (#PSS.view == math.min(PSS.c,#indexes))
		--PSS.print_view("initial view")
	end,

	
	log_view = function()
		-- called once to log the view
		events.sleep(10.5*PSS_SHUFFLE_PERIOD)
		log:print("VIEW_CONTENT "..job.position.." "..PSS.set_of_peers_to_string(PSS.view))
	end,

}

-- ############################################################################
-- 	T-CHORD
-- ############################################################################


TCHORD = {
	leaves = {},
	fingers = {},
	t = TCHORD_EXPONENT,
	l = TCHORD_LEAF,
	r = TCHORD_RANDOM,
	timeout = TCHORD_HB_TIMEOUT,
	leaves_lock = events.lock(),
	fingers_lock = events.lock(),
	-- current cycle of the active TCHORD thread
	cycle = 0,
	
-------------------------------------------------------------------------------
-- debug
-------------------------------------------------------------------------------
	display_view = function(v, which)
 		local display = table.concat({which," VIEW_CONTENT ",me.id,"(", me.id%2^TCHORD.t,")\t"})
		for i,w in ipairs(v) do
			display = table.concat({display ," ",w.id,"(",w.id%2^TCHORD.t,")"})
		end
		log:print(display)
	end,

	
	display_leaves = function(c) 
		local out = table.concat({"TCHORD cycle", " ", c, " ", "APPROX. LEAVES\t"})
		for i = 1, TCHORD.l do
			if TCHORD.leaves[i] then
				out = table.concat({out,"l",i," ",TCHORD.leaves[i].id,"(",TCHORD.leaves[i].id%2^TCHORD.t,")\t"})
			end
		end
		log:print(out)
	end,
	
	display_fingers = function(c) 
		local out = table.concat({"TCHORD cycle", " ", c, " ", "APPROX. FINGERS\t"})
		for i = 1, TCHORD.t do
			if TCHORD.fingers[i] then
				out = table.concat({out,"f",(i-1)," ",TCHORD.fingers[i].id,"(",(TCHORD.fingers[i].id)%2^TCHORD.t,")\t"})
			end
		end
		log:print(out)
	end,

	debug = function(c)
		TCHORD.display_leaves(c)
		TCHORD.display_fingers(c)
	end,
	
-------------------------------------------------------------------------------
-- utilities
-------------------------------------------------------------------------------
	-- remove duplicates from view
	remove_dup = function(set)
		--local rd = misc.time()
		for i,v in ipairs(set) do
			local j = i+1
			while(j <= #set and #set > 0) do
				if v.id == set[j].id then
					table.remove(set,j)
				else j = j + 1
				end
			end
		end
		--log:print("TCHORD.remove_dup", misc.time()-rd)
	end,

	--keep n first elelements from t
	keep_n = function(t,n)
		for i = #t, n+1, -1 do
			table.remove(t,i)
		end
	end,

	remove_self = function(set, partner)
		for i,v in ipairs(set) do
			if v.peer.port == partner.peer.port and v.peer.ip == partner.peer.ip then table.remove(set, i) end
		end
	end,

	--ranks the set of nodes from the point of view of the node with the specified id, based on their clockwise distance on the ring
	rank = function(n, set)
		local distances = {}
		local ranked = {}
		for i,v in ipairs(set) do
			local dist = v.id%2^TCHORD.t - n.id%2^TCHORD.t 
			local d = 0
			if dist >= 0 then d = dist
			else d = dist + 2^TCHORD.t end
			distances[#distances+1] = {distance= d, node=v}
		end
		table.sort(distances, function(a,b) return a.distance < b.distance end)
		for i,v in ipairs(distances) do
			ranked[#ranked+1] = v.node
		end
		return ranked
	end,

	--splits set into successors and predecessors of partner on the ring
	split_succ_pred = function(set,partner)
		local succ, pred = {}, {}
		for i,v in ipairs(set) do
			if (v.id-partner.id+2^TCHORD.t)%2^TCHORD.t < 2^(TCHORD.t-1) then
				succ[#succ+1] = v
			else 
				pred[#pred+1] = v
			end
		end
		return succ, pred
	end,
	
	remove_failed_node = function(node)
	
		TCHORD.leaves_lock:lock()
		TCHORD.remove_node(TCHORD.leaves, node)
		TCHORD.leaves_lock:lock()
		
		TCHORD.fingers_lock:lock()
		TCHORD.remove_node(TCHORD.fingers, node)
		TCHORD.fingers_lock:unlock()
		
	end, 
	
	remove_node = function(set, node)
		for i,v in ipairs(set) do
			if TCHORD.same_node(v, node) then
				table.remove(set, i)
				break
			end
		end
	end,
	
	same_node = function(n1,n2)
		local peer_first
		if n1.peer then peer_first = n1.peer else peer_first = n1 end
		local peer_second
		if n2.peer then peer_second = n2.peer else peer_second = n2 end
		return peer_first.port == peer_second.port and peer_first.ip == peer_second.ip
	end,
	
	extract_finger_candidates = function(set,p)
		local finger_candidates = {}
		for j = 1, TCHORD.t do        
			finger_candidates[j] = {}
			local b = (p.id%2^TCHORD.t+ 2^j)%2^TCHORD.t
			local a = (p.id%2^TCHORD.t + 2^(j-1)-1)%2^TCHORD.t
			for i, v in ipairs(set) do
				local c = v.id%2^TCHORD.t
				if a <= b then 
					if c > a and c <= b then
						finger_candidates[j][#finger_candidates[j]+1] = v
					end
				else
					if (c > a and c < 2^TCHORD.t) or (c <= b and c >= 0) then
						finger_candidates[j][#finger_candidates[j]+1] = v
					end
				end
			end
		end
		return finger_candidates
	end,
	
	--select the closest following node from set
	select_closest = function(p, set)
		local ranked = TCHORD.rank(p, set)
		TCHORD.remove_self(ranked, p)
		for _,v in ipairs(ranked) do
			if v.id%2^TCHORD.t ~= p.id%2^TCHORD.t then return v end
		end 
	end,
	
	update_age_merge = function(t1,t2)
		local new_entries = {}
		local matched = {}
		local is_new = true
		for i,v in ipairs(t1) do
			for j,w in ipairs(t2) do
				if TCHORD.same_node(w,v) then
					TCHORD.update_age (w,v)
					matched[j] = true
					is_new = false
					break
				end
			end
			if is_new then new_entries[#new_entries+1] = v end
		end
		for i,v in ipairs(t2) do
			if not matched[i] then
				v.age = v.age+1
			end
		end
		local merged = misc.merge(t2, new_entries)
		TCHORD.remove_stale_nodes(merged)
		return merged
	end,
	
	remove_stale_nodes = function(set)
		for i,v in ipairs(set) do
			if TCHORD.is_stale(v) then table.remove(set, i) end
		end
	end,
	
	is_stale = function(node)
		if  node.age >= TCHORD.timeout then return true
		else return false end
	end,
	
	update_age  = function(n1,n2)
		if n1.age > n2.age then n1.age = n2.age end
	end,

-------------------------------------------------------------------------------
-- T-Chord
-------------------------------------------------------------------------------

-- using PSS for initialising leaves 
	init = function()
		--local startTCHORDInit = misc.time()
		PSS.view_copy_lock:lock()		
		TCHORD.leaves = misc.random_pick(PSS.view_copy, TCHORD.l)
		PSS.view_copy_lock:unlock()
		TCHORD.leaves = TCHORD.rank(me,TCHORD.leaves)
		--log:print("TCHORD.init", misc.time()-startTCHORDInit)
	end,

	select_peer = function()
		--local sp = misc.time()
		local partner = TCHORD.leaves[math.random(TCHORD.l)]
		--log:print("TCHORD.select_peer", misc.time() - sp, "peer id", partner.id%2^TCHORD.t)
		return partner
		end,

	create_message = function(partner)	
		--local cm = misc.time()
		PSS.view_copy_lock:lock()		
		local pss_buffer = misc.random_pick(PSS.view_copy, TCHORD.r)
		PSS.view_copy_lock:unlock()
		TCHORD.leaves_lock:lock()
		local merged =  misc.merge(TCHORD.leaves, pss_buffer)
		TCHORD.leaves_lock:unlock()
		--TCHORD.display_view(merged, "create_message, merged for partner "..partner.id%2^TCHORD.t)
		TCHORD.remove_self(merged, partner)
		merged[#merged+1] = me
		TCHORD.remove_dup(merged)
		merged = TCHORD.rank(partner, merged)
	--keep the first l nodes in the message
		TCHORD.keep_n(merged,TCHORD.l)
		--log:print("TCHORD.create_message", misc.time() - cm)
		return merged	
	end,

--improve current leaf set using received 
	update_leaf_set = function(received)
		--local ul = misc.time()
		TCHORD.leaves_lock:lock()
		local merged = TCHORD.update_age_merge(received,TCHORD.leaves)
		merged = TCHORD.rank(me, merged)
	-- split into successors and predecessors of self on the ring
		local succ, pred = TCHORD.split_succ_pred(merged,me)
	-- if there are enough successors, keep the first l for the leaves, otherwise fill in the missing leaves from the predecessors
		if #succ > TCHORD.l then
			TCHORD.keep_n(succ, TCHORD.l)
			TCHORD.leaves = succ
		elseif #succ < TCHORD.l and #pred > 0 then
			TCHORD.leaves = succ
			for i = #pred, #pred-TCHORD.l-#succ+1, -1 do
				TCHORD.leaves[#TCHORD.leaves+1] = pred[i]
			end
		else TCHORD.leaves = succ end
		TCHORD.leaves_lock:unlock()
		--log:print("TCHORD.update_leaf_set", misc.time() - ul)
	end,
	
	update_routing_table = function(received)
		local finger_candidates = TCHORD.extract_finger_candidates(received,me)
		TCHORD.fingers_lock:lock()
		for i,v in ipairs(finger_candidates) do
			if TCHORD.fingers[i] then
				v = TCHORD.update_age_merge(v, {TCHORD.fingers[i]})
			end
			if #v > 0 then
				TCHORD.fingers[i] = TCHORD.select_closest(me, v)
			end
		end
		TCHORD.fingers_lock:unlock()
	end,

	passive_thread = function(received,sender)
		local buffer = TCHORD.create_message(sender)
		TCHORD.update_leaf_set(received)
		TCHORD.update_routing_table(received)
		return buffer
	end,

	active_thread = function()
		local partner = TCHORD.select_peer()
		local buffer = TCHORD.create_message(partner)		
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'TCHORD.passive_thread', buffer, me})
		while not ok do
			try = try + 1
			if try <= 3 then
				log:print("TCHORD active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 3, try * 6))
				ok, res = rpc.acall(partner.peer, {'TCHORD.passive_thread', buffer, me})
			else
				log:print("TCHORD active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
				log:print("TKELIPS active thread: removing non-responding node from AG VIEW")
				TCHORD.remove_failed_node(partner)
			end
		end
		if ok then
			local received = res[1]
			local loc_cycle = TCHORD.cycle +1
			TCHORD.cycle = TCHORD.cycle + 1
			TCHORD.update_leaf_set(received)
			TCHORD.update_routing_table(received)
			TCHORD.debug(loc_cycle)
		end
	end,
	
}

-------------------------------------------------------------------------------
-- main loop
-------------------------------------------------------------------------------
max_time = 180

function terminator()
  events.sleep(max_time)
  os.exit()
end

function main()
-- this thread will be in charge of killing the node after max_time seconds
	events.thread(terminator)
	log:print("UP: "..job.me.ip..":"..job.me.port)
	log:print(table.concat({"ME: ", me.id, " (", me.id%2^TCHORD.t, ")"}))
-- initialize the peer sampling service
	PSS.pss_init()
-- init random number generator
-- init random number generator
	math.randomseed(job.position*os.time())
-- wait for all nodes to start up (conservative)
	events.sleep(2)
-- desynchronize the nodes
	local desync_wait = (GOSSIP_TIME * math.random())
	log:print("waiting for "..desync_wait.." to desynchronize")
	events.sleep(desync_wait) 
--initializing the leaf set
	TCHORD.init() 
--launching TCHORD
	tchord_thread = events.periodic(GOSSIP_TIME, TCHORD.active_thread)
		
end  

events.thread(main)  
events.loop()
