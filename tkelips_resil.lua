-------------------------------------------------------------------------------
-- modules and parameters
-------------------------------------------------------------------------------

require"splay.base"
rpc = require"splay.rpc"
misc = require"splay.misc"
crypto = require"crypto"

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

--size of the network
n = tonumber(arg[2])
--number of affinity groups
k = math.floor(math.sqrt(n))


GOSSIP_TIME= tonumber(PARAMS["GOSSIP_TIME"]) or 5
TKELIPS_RANDOM = tonumber(PARAMS["TKELIPS_RANDOM"]) or 3
TKELIPS_MESSAGE_SIZE = tonumber(PARAMS["TKELIPS_MESSAGE_SIZE"]) or 6
TKELIPS_CONTACT_SIZE = tonumber(PARAMS["TKELIPS_CONTACT_SIZE"]) or 1
TKELIPS_HB_TIMEOUT = tonumber(PARAMS["TKELIPS_HB_TIMEOUT"]) or 100
TKELIPS_AG_SIZE = n/k

PSS_VIEW_SIZE =tonumber(PARAMS["PSS_VIEW_SIZE"]) or 10
PSS_SHUFFLE_SIZE =  tonumber(PARAMS["PSS_SHUFFLE_SIZE"]) or math.floor(PSS_VIEW_SIZE / 2 + 0.5)
PSS_SHUFFLE_PERIOD = tonumber(PARAMS["PSS_SHUFFLE_PERIOD"]) or 20


-------------------------------------------------------------------------------
-- current node
-------------------------------------------------------------------------------
me = {}
me.peer = job.me


M = 32
function compute_hash(o)
	return tonumber(string.sub(crypto.evp.new("sha1"):digest(o), 1, M/4), 16)
end

me.id = compute_hash(table.concat({tostring(job.me.ip),":",tostring(job.me.port)}))
me.age = 0
-- affinity group
ag = me.id%k

-------------------------------------------------------------------------------
-- Peer Sampling Service
-------------------------------------------------------------------------------

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
		PSS.print_view("initial view")
	end,

	
	log_view = function()
		-- called once to log the view
		events.sleep(10.5*PSS_SHUFFLE_PERIOD)
		log:print("VIEW_CONTENT "..job.position.." "..PSS.set_of_peers_to_string(PSS.view))
	end,

}

-------------------------------------------------------------------------------
-- T-KELIPS
-------------------------------------------------------------------------------

TKELIPS = {
	aff_group= {},
	contacts = {},
	filetuples = {},
	t = TKELIPS_AG_SIZE,
	c = TKELIPS_CONTACT_SIZE,
	r = TKELIPS_RANDOM,
	m = TKELIPS_MESSAGE_SIZE,
	timeout = TKELIPS_HB_TIMEOUT,
	cycle = 0,
	
-------------------------------------------------------------------------------
-- debug
-------------------------------------------------------------------------------	
	
	display_view = function(v, which)
 		local display = table.concat({which," VIEW_CONTENT\t"})
		for i,w in ipairs(v) do
			display = table.concat({display ," ", w.id, "(", w.id%k, ")"})
		end
		log:print(display)
	end,

	
	display_ag = function(c) 
		local out = table.concat({"TKELIPS cycle", " ", c, " ", "AFFINITY GROUP VIEW\t"})
		for i = 1, TKELIPS.t do
			if TKELIPS.aff_group[i] then
				out = table.concat({out, i," ",TKELIPS.aff_group[i].id,"(",TKELIPS.aff_group[i].id%k,")\t"})
			end
		end
		log:print(out)
	end,
	
	display_contacts = function(c)
		local out = table.concat({"TKELIPS cycle", " ", c, " ", "CONTACTS\n"})
		for i = 0, k-1 do
			if TKELIPS.contacts[i] then
			local str = table.concat({i,": "})
				for j = 1, TKELIPS.c do
					if TKELIPS.contacts[i][j] then
						str = table.concat({str," ",TKELIPS.contacts[i][j].id,"(",TKELIPS.contacts[i][j].id%k,")"})
					else
						str = table.concat({str," -"})
					end
				end
			out = table.concat({out,str,"\n"})
			end
		end
		log:print(out)
	end,

	debug = function(c)
		TKELIPS.display_ag(c)
		TKELIPS.display_contacts(c)
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
		--log:print("TKELIPS.remove_dup", misc.time()-rd)
	end,

	--remove elements from table starting from index n+1 
	keep_n = function(t,n)
		for i = #t, n+1, -1 do
			table.remove(t,i)
		end
	end,

	remove_node = function(set, partner)
		for i,v in ipairs(set) do
			if TKELIPS.same_node(v, partner) then
				table.remove(set, i)
				break
			end
		end
	end,

	--ranks nodes from set according to the distance on the ring relative to n
	rank = function(n, set)
		local distances = {}
		local ranked = {}
		for i,v in ipairs(set) do
			local d = 0
			local dist_clockwise = math.abs(v.id%k - n.id%k)
			local dist_counter = k - dist_clockwise
			if dist_clockwise <= dist_counter then d = dist_clockwise
			else d = dist_counter end
			distances[#distances+1] = {distance= d, node=v}
		end
		table.sort(distances, function(a,b) return a.distance < b.distance end)
		for i,v in ipairs(distances) do
			ranked[#ranked+1] = v.node
		end
		return ranked
	end,
	
	--ranks nodes in set according to their age
	rank_by_age = function(set)
		table.sort(set, function(a,b) return a.age < b.age end)
	end,
	
	random_pick = function(n)
		local result = {}
		for i = 1,n do
			repeat index = math.random(1,#job.nodes)
			until (index ~= job.position)
			local a_peer = job.nodes[index]
			local hashed_index = compute_hash(table.concat({tostring(a_peer.ip),":",tostring(a_peer.port)}))
			result[#result+1] = {peer=a_peer, id=hashed_index, age = 0}
		end
		return result	
	end,
	
	--return nodes from received that belong to the affinity group ag
	filter_ag = function(received, ag)
		local filtered = {}
		for _,v in ipairs(received) do
			if v.id%k == ag then
				filtered[#filtered+1] = v
			end
		end
		return filtered
	end,
	
	same_node = function(n1,n2)
		if n1.peer.port ==n2.peer.port and n1.peer.ip == n2.peer.ip then
			return true
		else
			return false
		end
	end,
		
	remove_failed_node = function(node)
		TKELIPS.remove_from_ag(node)
		TKELIPS.remove_from_contacts(node)
	end,
	
	remove_from_ag = function(node)
		TKELIPS.remove_node(TKELIPS.aff_group, node)
	end,
	
	remove_from_contacts = function(node)
		for i = 0, k-1 do
			if TKELIPS.contacts[i] then
				for j = 1, TKELIPS.c do
					if TKELIPS.contacts[i][j] then
						if TKELIPS.same_node(TKELIPS.contacts[i][j], node) then
							table.remove(TKELIPS.contacts[i], j)
							break
						end
					end
				end
			end
		end
	end,
	
	remove_stale_nodes = function(set)
		for i,v in ipairs(set) do
			if TKELIPS.is_stale(v) then table.remove(set, i) end
		end
	end,
	
	is_stale = function(node)
		if  node.age >= TKELIPS.timeout then return true
		else return false end
	end,
	
	update_age  = function(n1,n2)
		if n1.age > n2.age then n1.age = n2.age end
	end,
	

-------------------------------------------------------------------------------
-- T-MAN
-------------------------------------------------------------------------------

	select_peer = function()
		local partner = PSS.pss_getPeer()
		return partner
		end,

	create_message = function(partner)
		local merged =  misc.merge(TKELIPS.aff_group, misc.random_pick(PSS.view, TKELIPS.r))
		TKELIPS.remove_node(merged, partner)
		merged[#merged+1] = me
		TKELIPS.remove_dup(merged)
		merged = TKELIPS.rank(partner, merged)
		TKELIPS.keep_n(merged,TKELIPS.m)
		return merged	
	end,

	update_aff_group = function(received)
		local ag_candidates = TKELIPS.filter_ag(received, ag)
		local new_entries = {}
		local matched = {}
		local is_new = true
		for i,v in ipairs(ag_candidates) do
			for j,w in ipairs(TKELIPS.aff_group) do
				if TKELIPS.same_node(w,v) then
					TKELIPS.update_age (w,v)
					matched[j] = true
					is_new = false
					break
				end
			end
			if is_new then new_entries[#new_entries+1] = v end
		end
		for i,v in ipairs(TKELIPS.aff_group) do
			if not matched[i] then
				v.age = v.age+1
			end
		end
		local merged = misc.merge(TKELIPS.aff_group, new_entries)
		TKELIPS.aff_group = merged
		TKELIPS.remove_stale_nodes(TKELIPS.aff_group)
	end,

	update_contacts = function(received)
		for i,v in ipairs(received) do
			local ag = v.id%k
			local is_new = true
			if TKELIPS.contacts[ag] == nil then
				TKELIPS.contacts[ag] = {}
			end
			for j,w in ipairs(TKELIPS.contacts[ag]) do
				if TKELIPS.same_node(v,w) then
					TKELIPS.update_age (w,v)
					is_new = false
					break
				end
			end		
			if is_new then
				local t = {v}
				local merged = misc.merge(TKELIPS.contacts[ag], t)
				TKELIPS.rank_by_age(merged)
				TKELIPS.keep_n(merged, TKELIPS.c)
				TKELIPS.contacts[ag] = merged
				TKELIPS.remove_stale_nodes(TKELIPS.contacts[ag])
			end		
		end
	end,

	passive_thread = function(received,sender)
		local buffer = TKELIPS.create_message(sender)
		TKELIPS.update_aff_group(received)
		TKELIPS.update_contacts(received)
		return buffer
	end,

	active_thread = function()
		local partner = TKELIPS.select_peer()
		local buffer = TKELIPS.create_message(partner)
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'TKELIPS.passive_thread', buffer, me})
		while not ok do
			try = try + 1
			if try <= 2 then
				log:print("TMAN active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 3, try * 6))
				ok, res = rpc.acall(partner.peer, {'TKELIPS.passive_thread', buffer, me})
			else
				log:print("TMAN active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
				TKELIPS.remove_failed_node(partner)
			end
		end
		if ok then
			local received = res[1]
			local loc_cycle = TKELIPS.cycle+1
			TKELIPS.cycle = TKELIPS.cycle+1
			TKELIPS.update_aff_group(received)
			TKELIPS.update_contacts(received)
			TKELIPS.debug(loc_cycle)
		end
	end,
	
}

-------------------------------------------------------------------------------
-- main loop
-------------------------------------------------------------------------------
max_time = 360

function terminator()
  events.sleep(max_time)
  os.exit()
end

function main()
-- this thread will be in charge of killing the node after max_time seconds
	events.thread(terminator)
	log:print("UP: "..job.me.ip..":"..job.me.port)
	log:print(table.concat({"ME: ", me.id, " (", me.id%k, ")"}))
-- initialize the peer sampling service
	PSS.pss_init()
-- init random number generator
	math.randomseed(job.position*os.time())
-- wait for all nodes to start up (conservative)
	events.sleep(2)
-- desynchronize the nodes
	local desync_wait = (GOSSIP_TIME * math.random())
	log:print("waiting for "..desync_wait.." to desynchronize")
	events.sleep(desync_wait) 
	pss_thread = events.periodic(PSS_SHUFFLE_PERIOD, PSS.pss_active_thread) 
	events.sleep(60)
	tman_thread = events.periodic(GOSSIP_TIME, TKELIPS.active_thread)
			
end  

events.thread(main)  
events.loop()
