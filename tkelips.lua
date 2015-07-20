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
n = 128
--number of affinity groups
k = math.floor(math.sqrt(n))

--TKELIPS params
GOSSIP_TIME= tonumber(PARAMS["GOSSIP_TIME"]) or 5
TKELIPS_RANDOM = tonumber(PARAMS["TKELIPS_RANDOM"]) or 6
TKELIPS_MESSAGE_SIZE = tonumber(PARAMS["TKELIPS_MESSAGE_SIZE"]) or 10
TKELIPS_CONTACT_SIZE = tonumber(PARAMS["TKELIPS_CONTACT_SIZE"]) or 2
TKELIPS_HB_TIMEOUT = tonumber(PARAMS["TKELIPS_HB_TIMEOUT"]) or 300
TKELIPS_CONVERGE = PARAMS["TKELIPS_CONVERGE"] or true

--KELIPS lookup params
KELIPS_LOOKUP_TEST = PARAMS["KELIPS_LOOKUP_TEST"] or false

--PSS params
PSS_VIEW_SIZE =tonumber(PARAMS["PSS_VIEW_SIZE"]) or 10
PSS_SHUFFLE_SIZE =  tonumber(PARAMS["PSS_SHUFFLE_SIZE"]) or math.floor(PSS_VIEW_SIZE / 2 + 0.5)
PSS_SHUFFLE_PERIOD = tonumber(PARAMS["PSS_SHUFFLE_PERIOD"]) or 10


-------------------------------------------------------------------------------
-- current node
-------------------------------------------------------------------------------
me = {}
me.peer = job.me

--current node's id

M = 32
function compute_hash(o)
	return tonumber(string.sub(crypto.evp.new("sha1"):digest(o), 1, M/4), 16)
end
me.id = compute_hash(table.concat({tostring(job.me.ip),":",tostring(job.me.port)}))

--current node's age
me.age = 0


-- current node's affinity group
function get_ag(node)
	if type(node) == "table" then return node.id%n%k
	else return node%n%k end
end
ag = get_ag(me)
	

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
-- 	T-KELIPS
-- ############################################################################

TKELIPS = {
	complete_aff_group = {},
	aff_group= {},
	contacts = {},
	filetuples = {},
	c = TKELIPS_CONTACT_SIZE,
	--r = TKELIPS_RANDOM,
	m = TKELIPS_MESSAGE_SIZE,
	timeout = TKELIPS_HB_TIMEOUT,
	aff_group_lock = events.lock(),
	contacts_lock = events.lock(),
	ft_lock = events.lock(),
	cycle = 0,
	
-------------------------------------------------------------------------------
-- debug
-------------------------------------------------------------------------------	
	
	display_view = function(v, which)
 		local display = table.concat({which," VIEW_CONTENT " ..me.id..":\t"})
		for i,w in ipairs(v) do
			display = table.concat({display ," ", w.id, "(", get_ag(w), ")"})
		end
		log:print(display)
	end,

	
	display_ag = function(c) 
		local out = table.concat({"TKELIPS cycle", " ", c, " ", "AG VIEW "..me.id..":\t"})
		for i,v in ipairs(TKELIPS.aff_group) do
			if v then
				out = table.concat({out, i," ",v.id,"(",get_ag(v),") "})
			end
		end
		log:print(out)
	end,
	
	display_contacts = function(c)
		local out = table.concat({"TKELIPS cycle", " ", c, " ", "CONTACTS "..me.id..":\n"})
		for i = 0, k-1 do
			if TKELIPS.contacts[i] then
			local str = table.concat({i,": "})
				for j = 1, TKELIPS.c do
					if TKELIPS.contacts[i][j] then
						str = table.concat({str," ",TKELIPS.contacts[i][j].id,"(",get_ag(TKELIPS.contacts[i][j]),")"})
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

	same_node = function(n1,n2)
		local peer_first
		if n1.peer then peer_first = n1.peer else peer_first = n1 end
		local peer_second
		if n2.peer then peer_second = n2.peer else peer_second = n2 end
		return peer_first.port == peer_second.port and peer_first.ip == peer_second.ip
	end,
	
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

	--ranks nodes in set according to the distance on the ring relative to n
	rank = function(n, set)
		local distances = {}
		local ranked = {}
		for i,v in ipairs(set) do
			local d = 0
			local dist_clockwise = math.abs(get_ag(v) - get_ag(n))
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
	
	--ranks nodes from set according to the counter-clockwise distance on the ring relative to node
	rank_unused = function(node, set)
		local distances = {}
		local ranked = {}
		for i,v in ipairs(set) do
			local d = n - math.abs(v.id%n - node.id%n)
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
	
	precompute_aff_group = function()
		if TKELIPS_CONVERGE then
			for i,v in ipairs(job.nodes) do
				local id = compute_hash(table.concat({tostring(v.ip),":",tostring(v.port)}))
				if get_ag(id) == ag and (not TKELIPS.same_node(v, me)) then
					TKELIPS.complete_aff_group[#TKELIPS.complete_aff_group+1] = v
				end
			end
		end
	end,
	
	--return nodes from received that belong to the affinity group ag
	filter_ag = function(received, ag)
		local filtered = {}
		for _,v in ipairs(received) do
			if get_ag(v) == ag then
				filtered[#filtered+1] = v
			end
		end
		return filtered
	end,
		
	remove_failed_node = function(node)
		TKELIPS.remove_from_ag(node)
		TKELIPS.remove_from_contacts(node)
	end,
	
	remove_from_ag = function(node)
		TKELIPS.aff_group_lock:lock()
		TKELIPS.remove_node(TKELIPS.aff_group, node)
		TKELIPS.aff_group_lock:unlock()
	end,
	
	remove_from_contacts = function(node)
		TKELIPS.contacts_lock:lock()
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
		TKELIPS.contacts_lock:unlock()
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
-- convergence check
-------------------------------------------------------------------------------
	
	check_convergence = function()
		local found_contacts = {TKELIPS.check_contacts_convergence()}
		TKELIPS.aff_group_lock:lock()
		local mandatory = found_contacts[1] + #TKELIPS.aff_group
		TKELIPS.aff_group_lock:unlock()
		local optional = found_contacts[2]
		log:print("CURRENT VIEW STATE "..me.id.." mandatory_entries:".. mandatory .." optional_entries:"..optional)
	end,
	
	check_contacts_convergence = function()
		local mandatory = 0
		local missing_optional = 0
		for i = 0, k-1 do
			if TKELIPS.contacts[i] then
				mandatory = mandatory + 1
				for j = 1, TKELIPS.c do
					if not TKELIPS.contacts[i][j] then 
						missing_optional = missing_optional + 1
					end
				end
			else missing_optional = missing_optional + TKELIPS.c-1 end
		end
		local optional = k*(TKELIPS.c-1) - missing_optional
		return mandatory, optional
	end,
	
-------------------------------------------------------------------------------
-- T-MAN functions
-------------------------------------------------------------------------------

	select_peer = function()
		if #TKELIPS.aff_group > 0 then 
			return misc.random_pick(TKELIPS.aff_group)
		else return PSS.pss_getPeer() end
			--return PSS.pss_getPeer()
		end,

	create_message = function(partner)
		--get r nodes from PSS, excluding the partner
		PSS.view_copy_lock:lock()
		local buffer_from_pss = misc.dup(PSS.view_copy)
		PSS.view_copy_lock:unlock()
		
		TKELIPS.aff_group_lock:lock()
		local buffer_from_ag = misc.dup(TKELIPS.aff_group)
		TKELIPS.aff_group_lock:unlock()	
		
		local res = misc.merge(buffer_from_pss, buffer_from_ag)
		TKELIPS.remove_dup(res)
		TKELIPS.remove_node(res, partner)
		res = misc.random_pick(res, TKELIPS.m)
		return res
	end,

	update_aff_group = function(received)
		TKELIPS.aff_group_lock:lock()
		--log:print("aff_group_lock acquired "..misc.time())
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
			is_new = true
		end
		for i,v in ipairs(TKELIPS.aff_group) do
			if not matched[i] then
				v.age = v.age+1
			end
		end
		local merged = misc.merge(TKELIPS.aff_group, new_entries)
		TKELIPS.aff_group = merged
		TKELIPS.remove_stale_nodes(TKELIPS.aff_group)
		TKELIPS.aff_group_lock:unlock()
		--log:print("aff_group_lock released "..misc.time())
	end,

	update_contacts = function(received)
		TKELIPS.contacts_lock:lock()
		for i,v in ipairs(received) do
			local ag = get_ag(v)
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
			is_new = true
		end
		TKELIPS.contacts_lock:unlock()
	end,
	
	--ongoing_rpc=false,

	passive_thread = function(received,sender)
		--if TKELIPS.ongoing_rpc == true then return false end
		local buffer = TKELIPS.create_message(sender)
		TKELIPS.update_aff_group(received)
		TKELIPS.update_contacts(received)
		return buffer
	end,

	active_thread = function()
		--TKELIPS.ongoing_rpc = true
		local partner = TKELIPS.select_peer()
		local buffer = TKELIPS.create_message(partner)
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'TKELIPS.passive_thread', buffer, me})
		while not ok do
			try = try + 1
			if try <= 2 then
				log:print("TKELIPS active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 3, try * 6))
				ok, res = rpc.acall(partner.peer, {'TKELIPS.passive_thread', buffer, me})
			else
				log:print("TKELIPS active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
				log:print("TKELIPS active thread: removing non-responding node from AG VIEW")
				TKELIPS.remove_failed_node(partner)
				break
			end
		end
		if ok then
			local received = res[1]
			if received==false then
					log:print("TKELIPS received false due to ongoing RPC, will try again in a short while")
					events.sleep(math.random())	
					--the call was aborted due to pending RPC at peer's node
				else
					local loc_cycle = TKELIPS.cycle+1
					TKELIPS.cycle = TKELIPS.cycle+1
					TKELIPS.update_aff_group(received)
					TKELIPS.update_contacts(received)
					TKELIPS.check_convergence()
					TKELIPS.debug(loc_cycle)
				end		
		end
		--TKELIPS.ongoing_rpc = false
	end,
	
}

-- ############################################################################
-- 	KELIPS KEY INSERTION/LOOKUP
-- ############################################################################

KELIPS_LOOKUP = {
	
	display_ft = function()
		local out = table.concat({"FILETUPLES ", me.id, "(", get_ag(me.id), ")\n"})
		for i,v in pairs(TKELIPS.filetuples) do
			out = table.concat({out, i, " -> ", v.id, "\n"})
		end
		log:print(out)
	end,

	insert = function(key)
		local ag = get_ag(key)
		--lookup the closest node in contacts at the key's AG
		local closest = KELIPS_LOOKUP.find_closest(key, ag)
		--send insert request to the closest node
		if closest then
			local ok, res = rpc.acall(closest.peer, {'KELIPS_LOOKUP.insert_key', key})
			if ok then
				local homenode = res[1]
				if homenode then
					log:print("Key "..key.."(".. get_ag(key).."): inserted at node "..homenode.id.."("..get_ag(homenode)..")")
				end
			end
		else
			--no contact has been found
			log:print("Key "..key.."(".. get_ag(key).."): failed insertion")
		end
	end,
	
	-- TODO: return topologically closest contact
	find_closest = function(key, ag)
		TKELIPS.contacts_lock:lock()
		if TKELIPS.contacts[ag] then	
			return misc.random_pick (TKELIPS.contacts[ag])
		end
		TKELIPS.contacts_lock:unlock()
	end,

	insert_key = function(key)	
		--log:print("Received key for insertion "..key)
		TKELIPS.ft_lock:lock()
		if TKELIPS.filetuples[key] == nil then
		--randomly pick a node from the affinity group
			TKELIPS.aff_group_lock.lock()
			local homenode = misc.random_pick(TKELIPS.aff_group)
			TKELIPS.aff_group_lock.unlock()
			--insert homenode and key in filetuples
			--log:print("Inserting key "..key.. "->" ..homenode.id)
			TKELIPS.filetuples[key] = homenode
			return homenode
		end
		TKELIPS.ft_lock:unlock()
	end,

	active_thread = function()
		TKELIPS.aff_group_lock:lock()
		local partner = misc.random_pick(TKELIPS.aff_group)
		TKELIPS.aff_group_lock:unlock()
		local buffer = KELIPS_LOOKUP.createMessage()
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'KELIPS_LOOKUP.passive_thread', buffer})
		while not ok do
			try = try + 1
			if try <= 3 then
				log:print("Filetuples active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 3, try * 6))
				ok, res = rpc.acall(partner.peer, {'KELIPS_LOOKUP.passive_thread', buffer})
			else
				log:print("KELIPS_LOOKUP active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
				break
			end
		end
		if ok then
			local received = res[1]
			KELIPS_LOOKUP.update_file_tuples(received)	
			--KELIPS_LOOKUP.display_ft()
		end
	end,

	passive_thread = function(received)
		local buffer = KELIPS_LOOKUP.create_message()
		KELIPS_LOOKUP.update_file_tuples(received)
		return buffer
	end,

	create_message = function()
		--pick a random tuple from filetuples
		TKELIPS.ft_lock:lock()
		local keys = misc.table_keyset(TKELIPS.filetuples)
		local k = misc.random_pick(keys)
		local hn = TKELIPS.filetuples[k]
		TKELIPS.ft_lock:unlock()
		buffer = {key = k, homenode = hn}
		return buffer
	end,

	update_file_tuples = function(received)
		local key = received.key
		local homenode = received.homenode
		TKELIPS.ft_lock:lock()
		if TKELIPS.filetuples[key] == nil then
			TKELIPS.filetuples[key] = homenode
		end
		TKELIPS.ft_lock:unlock()
	end,

	init_file_tuples = function()
		local key = me.id
		TKELIPS.filetuples[key] = me
	end,

	lookup = function(key)
		local ag = get_ag(key)
		local closest = KELIPS_LOOKUP.find_closest(key, ag)
		local try = 0
		local ok, res = rpc.acall(closest.peer, {'KELIPS_LOOKUP.find_key', key})
		if ok then
			local homenode = res[1]
			if homenode then log:print("Key "..key.."(".. get_ag(key).."): found at node "..homenode.id.."("..get_ag(homenode)..")")
			else log:print("Key "..key.."(".. get_ag(key).."): lookup failure") end
		end
	end,

	find_key = function(key)
		TKELIPS.ft_lock:lock()
		local hn = TKELIPS.filetuples[key]
		TKELIPS.ft_lock:lock()
		return hn
	end,
	
	test_insert = function()
		local index = job.position%n + 1
		local a_peer = job.nodes[index]
		local key = compute_hash(table.concat({tostring(a_peer.ip),":",tostring(a_peer.port)}))+index
		log:print("Starting key insertion: "..key)
		KELIPS_LOOKUP.insert(key)
	end,
	
	test_lookup = function()
		local index = job.position%n + 1
		local a_peer = job.nodes[index]
		local key = compute_hash(table.concat({tostring(a_peer.ip),":",tostring(a_peer.port)}))+index
		log:print("Starting key lookup: "..key)
		KELIPS_LOOKUP.lookup(key)
	end

}



-------------------------------------------------------------------------------
-- main loop
-------------------------------------------------------------------------------
max_time = 300

function terminator()
  events.sleep(max_time)
  os.exit()
end

function main()
-- this thread will be in charge of killing the node after max_time seconds
	events.thread(terminator)
	log:print("UP: "..job.me.ip..":"..job.me.port)
	log:print(table.concat({"ME: ", me.id, " (", me.id%k, ")"}))
-- precompute current node affinity group for checking convergence
	TKELIPS.precompute_aff_group()
	log:print("COMPLETE VIEW STATE "..me.id.." mandatory_entries:".. #TKELIPS.complete_aff_group+k .." optional_entries:"..k*(TKELIPS.c-1))
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
	events.sleep(10)
	TKELIPS_thread = events.periodic(GOSSIP_TIME, TKELIPS.active_thread)
	
--	events.sleep(250)
	
-- test insertion and lookup
	if KELIPS_LOOKUP_TEST then
		KELIPS_LOOKUP.initFileTuples()
		KELIPS_LOOKUP.test_insert()
		events.sleep(20)
		kelips_lookup_thread = events.periodic(GOSSIP_TIME, KELIPS_LOOKUP.activeThread)
		events.sleep(120)
		KELIPS_LOOKUP.test_lookup()
	end
			
end  

events.thread(main)  
events.loop()
