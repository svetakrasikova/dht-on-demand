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

me = {}
me.peer = job.me
me.age = 0
function compute_id(o) return string.sub(crypto.evp.new("sha1"):digest(o), 1, bits / 4) end
me.id = compute_id(job.me.ip..job.me.port)
log:print("ME: "..me.id.." "..job.me.ip..":"..job.me.port)

-------------------------------------------------------------------------------
-- parameters
-------------------------------------------------------------------------------

--PSS params
PSS_VIEW_SIZE =tonumber(PARAMS["PSS_VIEW_SIZE"]) or 10
PSS_SHUFFLE_SIZE =  tonumber(PARAMS["PSS_SHUFFLE_SIZE"]) or math.floor(PSS_VIEW_SIZE / 2 + 0.5)
PSS_SHUFFLE_PERIOD = tonumber(PARAMS["PSS_SHUFFLE_PERIOD"]) or 10


max_time = 360
g_timeout = 60
actions = 0

--T-Pastry params
GOSSIP_TIME = tonumber(PARAMS["GOSSIP_TIME"]) or 5
TPASTRY_HB_TIMEOUT = tonumber(PARAMS["TCHORD_HB_TIMEOUT"]) or 300
TPASTRY_CONVERGE = PARAMS["TCHORD_CONVERGE"] or true
TPASTRY_RANDOM = tonumber(PARAMS["TPASTRY_RANDOM"]) or 6

b, leaf_size, bits = 4, 16, 128
key_size = math.log(2^bits)/ math.log(2^b)
if key_size < 2^b then
	print("Key size must be greater or equal than base")
	os.exit()
end
if b ~= 4 then
	print("b must be 4, because base 16 (hexadecimal) is needed in one function.")
	os.exit()
end

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
-- 	T-PASTRY
-- ############################################################################

TPASTRY = {
	r = TPASTRY_RANDOM,
	leaves_decreasing = {},
	leaves_increasing = {},
	routing_table = {},
	leaves_lock = events.lock(),
	rt_lock = events.lock(),
	cycle = 0,
	
-------------------------------------------------------------------------------
-- debug
-------------------------------------------------------------------------------
	
	display_view = function(v, which)
 		local display = table.concat({which," CONTENT for nodeID ",TPASTRY.num(me),": \t"})
		for i,w in ipairs(v) do
			display = table.concat({display," ",TPASTRY.num(w)})
		end
		log:print(display)
	end,
	
	display_route_table = function()
		local out = ""
		for i = 0, key_size / 4 do
			local str = table.concat({"",i,": "})
			for c = 0, 2^b - 1 do
				if TPASTRY.routing_table[i][c] then
					str = table.concat({str," ",TPASTRY.routing_table[i][c].id})
				else
					str = table.concat({str," -"})
				end
			end
			out = table.concat({out,str,"\n"})
		end
		return out
	end,

	display_leaf = function()
		local out = "DECREASING "
		for i = #TPASTRY.leaves_decreasing, 1, -1 do
			if TPASTRY.leaves_decreasing[i] then
				out = table.concat({out," (-)",TPASTRY.leaves_decreasing[i].id})
			end
		end
		out = table.concat({out," [",me.id,"] INCREASING "})
		for i = 1, #TPASTRY.leaves_increasing do
			if TPASTRY.leaves_increasing[i] then
				out = table.concat({out," (+)",TPASTRY.leaves_increasing[i].id})
			end
		end
		return out
	end,

	debug = function(c)
		log:print(table.concat({"TPASTRY cycle: ",c}))
		log:print(TPASTRY.display_route_table())
		log:print(TPASTRY.display_leaf())

	end,

	
-------------------------------------------------------------------------------
-- utilities
-------------------------------------------------------------------------------

	num = function(k)
		if k.id then return tonumber("0x"..k.id) else return tonumber("0x"..k) end
	end,

	diff = function(key1, key2)
		local k1, k2, a, b = TPASTRY.num(key1), TPASTRY.num(key2)
		if k1 < k2 then a, b = k2, k1 else a, b = k1, k2 end
		return math.min(a - b, 2^bits - a + b)
	end,

	shared_prefix_length = function(a, b)
		for i = 1, key_size do
			if string.sub(a, i, i) ~= string.sub(b, i, i) then return i - 1 end
		end
		return key_size
	end,

	row_col = function(key)
		local row = TPASTRY.shared_prefix_length(key, me.id)
		return row, TPASTRY.num(string.sub(key, row + 1, row + 1))
	end,

	row_col_partner = function(key,partner)
		local row = TPASTRY.shared_prefix_length(key, partner.id)
		return row, TPASTRY.num(string.sub(key, row + 1, row + 1))
	end,

	--sorts nodes according to the distance on the ring from the given node
	rank = function(t,partner)
		table.sort(t, function (a,b) return TPASTRY.diff(a, partner) < TPASTRY.diff(b, partner) end)
	end,

	--splits t into successors and predecessors of partner on the ring
	split_succ_pred = function(t,partner)
		local succ, pred = {}, {}
		for i,v in ipairs(t) do
			if (TPASTRY.num(v)-TPASTRY.num(partner)+2^bits)%2^bits < 2^(bits-1) then
				succ[#succ+1] = v
			else 
				pred[#pred+1] = v
			end
		end
		return succ, pred
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
		--log:print("TPASTRY.remove_dup", misc.time()-rd)
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
	
	random_pick = function(n)
		local result = {}
		for i = 1,n do
			repeat index = math.random(1,#job.get_live_nodes())
			until (index ~= job.position)
			local a_peer = job.get_live_nodes()[index]
			local hashed_index = compute_id(a_peer.ip..a_peer.port)
			result[#result+1] = {peer=a_peer, id=hashed_index}
		end
		return result	
	end,
	
-------------------------------------------------------------------------------
-- T-Pastry
-------------------------------------------------------------------------------
	
	--initialises leaf sets with leaf_size/2 random nodes from pss each
	TPASTRY_init = function()
		for i = 0, key_size - 1 do TPASTRY.routing_table[i] = {} end
		PSS.view_copy_lock:lock()
		TPASTRY.leaves_decreasing = misc.random_pick(PSS.view_copy, leaf_size/2)
		TPASTRY.leaves_increasing = misc.random_pick(PSS.view_copy, leaf_size/2)
		PSS.view_copy_lock:unlock()
	end,


	selectPeer = function()
		TPASTRY.leaves_lock:lock()
		local merged = misc.merge(TPASTRY.leaves_decreasing, TPASTRY.leaves_increasing)
		TPASTRY.leaves_lock:unlock()
		TPASTRY.rank(merged,me)
		return merged[math.random(#merged/2)]
	end,

	createMessage = function(partner)
		-- merge leaf sets with self
		TPASTRY.leaves_lock:lock()
		local merged = misc.merge(TPASTRY.leaves_decreasing, TPASTRY.leaves_increasing)
		TPASTRY.leaves_lock:lock()
		merged[#merged+1] = me
		--  add r random nodes from pss
		PSS.view_copy_lock:lock()
		merged = misc.merge(merged, misc.random_pick(PSS.view_copy, TPASTRY.r))
		PSS.view_copy_lock:unlock()
		TPASTRY.remove_self(merged,partner)
		TPASTRY.remove_dup(merged)
		--sort according to the distance to the partner
		TPASTRY.rank(merged,partner)
	-- keep the first leaf_size entries
		if #merged > leaf_size then TPASTRY.keep_n(merged, leaf_size) end
		return merged	
	end,
 
	updateLeafSet = function(received)
		TPASTRY.leaves_lock:lock()
	-- merge leaf sets with the received message and sort according to the distance to self on the ring
		local merged = misc.merge(TPASTRY.leaves_decreasing, TPASTRY.leaves_increasing, received)
	--remove duplicates	
		TPASTRY.remove_dup(merged)
		TPASTRY.rank(merged,me)
	-- split into successors and predecessors of self on the ring
		local succ, pred = TPASTRY.split_succ_pred(merged,me)
	-- if there are enough successors and predecessors keep only leaf_size/2 respectively,
	-- otherwise take more from the set with enough elements
		if #succ >= leaf_size/2 and #pred >= leaf_size/2 then
			TPASTRY.keep_n(succ,leaf_size/2)
			TPASTRY.leaves_increasing = succ
			TPASTRY.keep_n(pred,leaf_size/2)
			TPASTRY.leaves_decreasing = pred
		elseif #succ < leaf_size/2 then
			TPASTRY.leaves_increasing = succ
			TPASTRY.keep_n(pred, leaf_size/2+leaf_size/2-#succ)
			for i = #pred, leaf_size/2+1, -1 do
				TPASTRY.leaves_increasing[#TPASTRY.leaves_increasing+1] = table.remove(pred, i)
			end
			TPASTRY.leaves_decreasing = pred
		elseif #pred < leaf_size/2 then
			TPASTRY.leaves_decreasing = pred
			TPASTRY.keep_n(succ, leaf_size/2+leaf_size/2-#pred)
			for i = #succ, leaf_size/2+1, -1 do
				TPASTRY.leaves_decreasing[#TPASTRY.leaves_decreasing+1] = table.remove(succ, i)
			end
			TPASTRY.leaves_increasing = succ
		end
		TPASTRY.leaves_lock:unlock()
	end,

	-- fills in any missing table entries by the nodes from the received message
	updatePrefixTable = function(received)
		TPASTRY.rt_lock:lock()
		for i,v in ipairs(received) do
			local row, col = TPASTRY.row_col(v.id)
			if TPASTRY.routing_table[row] then
				if not TPASTRY.routing_table[row][col] then table.insert(TPASTRY.routing_table[row], col, v) end
			end
		end
		TPASTRY.rt_lock:unlock()
	end,

	passiveThread = function(received,sender)
		local buffer = TPASTRY.createMessage(sender)
		TPASTRY.updateLeafSet(received)
		TPASTRY.updatePrefixTable(received)
		return buffer
	end,

	activeThread = function()
		local partner = TPASTRY.selectPeer()
		local buffer = TPASTRY.createMessage(partner)
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'TPASTRY.passiveThread', buffer, me})
		while not ok do
			try = try + 1
			if try <= 3 then
				log:print("T-Pastry active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 30, try * 60))
				ok, res = rpc.acall(partner.peer, {'TPASTRY.passiveThread', buffer, me})
			else
				log:print("T-Pastry active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
				break
			end
		end
		if ok then
			local received = res[1]
			local loc_cycle = TPASTRY.cycle +1
			TPASTRY.cycle = TPASTRY.cycle + 1
			TPASTRY.updateLeafSet(received)	
			TPASTRY.updatePrefixTable(received)
			TPASTRY.debug(loc_cycle)
		end
	end,

}


-------------------------------------------------------------------------------
-- Main loop
-------------------------------------------------------------------------------
function terminator()
  events.sleep(max_time)
  os.exit()
end

function main()
-- this thread will be in charge of killing the node after max_time seconds
	events.thread(terminator)
	log:print("UP: "..job.me.ip..":"..job.me.port)
-- init random number generator
	math.randomseed(job.position*os.time())
-- wait for all nodes to start up (conservative)
  	events.sleep(2)
-- desynchronize the nodes
	local desync_wait = (GOSSIP_TIME * math.random())
  	log:print("waiting for "..desync_wait.." to desynchronize")
	events.sleep(desync_wait)   

	TPASTRY.TPASTRY_init()
	t2 = events.periodic(GOSSIP_TIME, TPASTRY.activeThread)
	
end

events.thread(main)
events.loop()
