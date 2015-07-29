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
bits = 52
function compute_id(o) return string.sub(crypto.evp.new("sha1"):digest(o), 1, bits/ 4) end

me = {}
me.peer = job.me
me.age = 0
me.id = compute_id(job.me.ip..job.me.port)

function num(k)
	if k.id then return tonumber("0x"..k.id) else return tonumber("0x"..k) end
end

-------------------------------------------------------------------------------
-- bitwise operations (from LuaBit v0.4)
-------------------------------------------------------------------------------

local function check_int(n)
 -- checking not float
 if(n - math.floor(n) > 0) then
  error("trying to use bitwise operation on non-integer!")
 end
end

local function to_bits(n)
 check_int(n)
 if(n < 0) then
  -- negative
  return to_bits(bit.bnot(math.abs(n)) + 1)
 end
 -- to bits table
 local tbl = {}
 local cnt = 1
 while (n > 0) do
  local last = n%2
  if(last == 1) then
   tbl[cnt] = 1
  else
   tbl[cnt] = 0
  end
  n = (n-last)/2
  cnt = cnt + 1
 end

 return tbl
end

local function tbl_to_number(tbl)
 local n = #tbl

 local rslt = 0
 local power = 1
 for i = 1, n do
  rslt = rslt + tbl[i]*power
  power = power*2
 end
 
 return rslt
end

local function expand(tbl_m, tbl_n)
 local big = {}
 local small = {}
 if #tbl_m > #tbl_n then
  big = tbl_m
  small = tbl_n
 else
  big = tbl_n
  small = tbl_m
 end
 -- expand small
 for i =#small + 1, #big do
  small[i] = 0
 end

end

local function bit_or(m, n)
 local tbl_m = to_bits(m)
 local tbl_n = to_bits(n)
 expand(tbl_m, tbl_n)

 local tbl = {}
 local rslt = math.max(#tbl_m, #tbl_n)
 for i = 1, rslt do
  if(tbl_m[i]== 0 and tbl_n[i] == 0) then
   tbl[i] = 0
  else
   tbl[i] = 1
  end
 end
 
 return tbl_to_number(tbl)
end

local function bit_and(m, n)
 local tbl_m = to_bits(m)
 local tbl_n = to_bits(n)
 expand(tbl_m, tbl_n) 

 local tbl = {}
 local rslt = math.max(#tbl_m, #tbl_n)
 for i = 1, rslt do
  if(tbl_m[i]== 0 or tbl_n[i] == 0) then
   tbl[i] = 0
  else
   tbl[i] = 1
  end
 end

 return tbl_to_number(tbl)
end

local function bit_not(n)
 
 local tbl = to_bits(n)
 local size = math.max(#tbl, 32)
 for i = 1, size do
  if(tbl[i] == 1) then 
   tbl[i] = 0
  else
   tbl[i] = 1
  end
 end
 return tbl_to_number(tbl)
end

local function bit_xor(m, n)
 local tbl_m = to_bits(m)
 local tbl_n = to_bits(n)
 expand(tbl_m, tbl_n) 

 local tbl = {}
 local rslt = math.max(#tbl_m, #tbl_n)
 for i = 1, rslt do
  if(tbl_m[i] ~= tbl_n[i]) then
   tbl[i] = 1
  else
   tbl[i] = 0
  end
 end
 

 return tbl_to_number(tbl)
end

local function bit_rshift(n, bits)
 check_int(n)
 
 local high_bit = 0
 if(n < 0) then
  -- negative
  n = bit_not(math.abs(n)) + 1
  high_bit = 2147483648 -- 0x80000000
 end

 for i=1, bits do
  n = n/2
  n = bit_or(math.floor(n), high_bit)
 end
 return math.floor(n)
end

-- logic rightshift assures zero filling shift
local function bit_logic_rshift(n, bits)
 check_int(n)
 if(n < 0) then
  -- negative
  n = bit_not(math.abs(n)) + 1
 end
 for i=1, bits do
  n = n/2
 end
 return math.floor(n)
end

local function bit_lshift(n, bits)
 check_int(n)
 
 if(n < 0) then
  -- negative
  n = bit_not(math.abs(n)) + 1
 end

 for i=1, bits do
  n = n*2
 end
 return bit_and(n, 4294967295) -- 0xFFFFFFFF
end

-------------------------------------------------------------------------------
-- parameters
-------------------------------------------------------------------------------
--T-KAD params
--size of TKAD message
TKAD_MESSAGE = tonumber(PARAMS["TKAD_MESSAGE"]) or 6
TKAD_VIEW = tonumber(PARAMS["TKAD_VIEW"]) or 10
GOSSIP_TIME = tonumber(PARAMS["GOSSIP_TIME"]) or 10
STATS_PERIOD = tonumber(PARAMS["STATS_PERIOD"]) or 10
K_SIZE = tonumber(PARAMS["K_SIZE"]) or 3
TKAD_RANDOM = tonumber(PARAMS["TKAD_RANDOM"]) or 5
TKAD_CONVERGE = PARAMS["TKAD_CONVERGE"] or true

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
				local hashed_index =  compute_id(a_peer.ip..a_peer.port)
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
-- 	T-KAD
-- ############################################################################


TKAD = {
	view = {},
	routing_table = {},
	v = TKAD_VIEW,
	r = TKAD_RANDOM,
	k = K_SIZE,
	m = TKAD_MESSAGE,
	view_lock = events.lock(),
	rt_lock = events.lock(),
	cycle = 0,
	ideal_rt = {},

-------------------------------------------------------------------------------
-- debug
-------------------------------------------------------------------------------

	display_view = function(v, which)
 		local display = table.concat({which,"\n"})
		for i,w in ipairs(v) do
			display = table.concat({display, " ",num(w),"(",TKAD.bit_out(w.id),")"})
		end
		log:print(display.."\n")
	end,

	bit_out = function(s)
		local out, tab = "", to_bits(num(s))
		for i = #tab, 1, -1 do
			out = table.concat({out,tab[i]})
		end
		return out
	end,
	
	display_rt = function()
		log:print("ROUTING TABLE:", TKAD.bit_out(me.id))
		for i = 0, bits do
			if TKAD.routing_table[i] and #TKAD.routing_table[i] > 0 then
				local out = ""
				for j,v in ipairs(routing_table[i]) do
					out = table.concat({out,TKAD.bit_out(v.id)," | "})
				end
				log:print(i, out)
			end
		end
	end,

	display_rt_bits = function()
		log:print("ROUTING TABLE:", num(me.id))
		for i = 0, bits do
			if TKAD.routing_table[i] and #TKAD.routing_table[i] > 0 then
				local out = ""
				for j,v in ipairs(TKAD.routing_table[i]) do
					out = table.concat({out,num(v.id)," | "})
				end
				log:print(i, out)
			end
		end
	end,

	debug = function(c,bits)
		log:print("TKAD cycle:", c)
		if bits == true then log:print(TKAD.display_rt_bits()) else log:print(TKAD.display_rt()) end
	end,

-------------------------------------------------------------------------------
-- utilities
-------------------------------------------------------------------------------

	remove_dup = function(set)
		for i,v in ipairs(set) do
			local j = i+1
			while(j <= #set and #set > 0) do
				if v.id == set[j].id then
					table.remove(set,j)
				else j = j + 1
				end
			end
		end
	end,

	--keep n first elelements from t
	keep_n = function(t,n)
		for i = #t, n+1, -1 do
			table.remove(t,i)
		end
	end,
	
	same_node = function(n1,n2)
		local peer_first
		if n1.peer then peer_first = n1.peer else peer_first = n1 end
		local peer_second
		if n2.peer then peer_second = n2.peer else peer_second = n2 end
		return peer_first.port == peer_second.port and peer_first.ip == peer_second.ip
	end,

	remove_self  = function(t, node)
		local j = 1
		for i = 1, #t do
			if TKAD.same_node(t[j],node) then table.remove(t, j)
			else j = j+1 end
		end
	end,
	
	-- computes the diff between two ids based on the number of bits in which they differ
	xor_diff = function(n,m)
		local xor_result = bit_xor(n,m)
		local diff = 0
		while xor_result > 0 do
			diff = diff + xor_result%2
			xor_result = bit_rshift(xor_result,1)
		end
		return diff
	end,

	--ranks nodes according to the number of differing bits in their IDs;
	--used for selecting TKAD peer and creating TKAD message
	xor_rank = function(set, partner)
		table.sort(set, function (a,b) return TKAD.xor_diff(num(a), num(partner)) < TKAD.xor_diff(num(b), num(partner)) end)
	end,


	xor_rank_pure = function(set, partner)
		table.sort(set, function (a,b) return bit_xor(num(a), num(partner)) < bit_xor(num(b), num(partner)) end)
	end,
	
	already_in = function(t,n)
		for i,v in ipairs(t) do
			if v.id == n.id then return i end
		end
		return -1
	end,
		
	--computes the number of the k-bucket for the given node
	bucket_num = function(p)
		local n = bit_xor(num(p), num(me))
		local counter = 0	
		while n ~= 0 do
			n = bit_logic_rshift(n, 1)
			counter = counter+1	
		end
		return bits-counter
	end,
	
	latency = function(n)
		local latency = rpc.ping(n.peer)
		if latency then return latency end
	end,
	
	
	remove_failed_node = function(node)
	
		TKAD.view_lock:lock()
		TKAD.remove_node(TKAD.view, node)
		TKAD.view_lock:unlock()	
		TKAD.rt_lock:lock()
		TKAD.remove_node(TKAD.routing_table, node)
		TKAD.rt_lock:unlock()
		
	end, 
	
	remove_node = function(set, node)
		for i,v in ipairs(set) do
			if TKAD.same_node(v, node) then
				table.remove(set, i)
				break
			end
		end
	end,
	
-------------------------------------------------------------------------------
-- Convergence
-------------------------------------------------------------------------------

	hash_all = function()
		local ids = {}
		for i,v in ipairs(job.nodes) do
			if not TKAD.same_node(v,me) then
				local hashed_index = compute_id(v.ip..v.port)
				ids[#ids+1] = hashed_index
			end
		end
		return ids
	end,


	precompute_routing_table = function()
		local num, entries = 0,{}
		if TKAD_CONVERGE then 
			for i = 0, bits do TKAD.ideal_rt[i] = {} end
			local ids = TKAD.hash_all()
			for i,v in ipairs(ids) do
				local buck = TKAD.bucket_num(v)
				if entries[buck] then entries[buck] = entries[buck]+1
				else entries[buck] = 1 end
				--print(v.id, TKAD.bit_out(v), buck) 
				table.insert(TKAD.ideal_rt[buck], v)		
			end
		end
		
		for i,v in pairs(entries) do
			if v > 3 then num = num + 3 else num = num + v end
		end
		log:print("COMPLETE VIEW STATE "..me.id.." mandatory_entries:".. 0 .." optional_entries:"..num)
	end,
	
	
	display_ideal_rt = function()
		log:print("IDEAL ROUTING TABLE:", num(me.id))
		for i = 0, bits do
			if TKAD.ideal_rt[i] and #TKAD.ideal_rt[i] > 0 then
				local out = ""
				for j,v in ipairs(TKAD.ideal_rt[i]) do
					out = out..num(v).." | "
				end
				log:print(i, out)
			end
		end
	end,

	check_convergence = function()
		local entries = 0
		for i = 0, #TKAD.routing_table do
			if TKAD.routing_table[i] and #TKAD.routing_table[i]>0 then
				for j,k in ipairs(TKAD.routing_table[i]) do
					local correct = false
					for l,m in ipairs(TKAD.ideal_rt[i]) do
						if num(k) == num(m) then
							correct = true
							break
						end
					end
					if correct then entries = entries + 1 end
				end
			end
		end
		log:print("CURRENT VIEW STATE "..me.id.." mandatory_entries:".. 0 .." optional_entries:"..entries)
		resource_stats()
	end,
-------------------------------------------------------------------------------
-- T-KAD
-------------------------------------------------------------------------------

	--initialises TKAD view using random pick from job.nodes()
	init = function()
		for i = 0, bits - 1 do TKAD.routing_table[i] = {} end
		PSS.view_copy_lock:lock()
		TKAD.view = misc.random_pick(PSS.view_copy, TKAD.v)
		PSS.view_copy_lock:unlock()
	end,

	

	-- ranks TKAD view according to xor_diff from self and selects a random node from the first m nodes
	select_peer = function()
		--log:print("Starting TKAD.select_peer")
		local sp = misc.time()
		if #TKAD.view > TKAD.m then
			--TKAD.xor_rank(TKAD.view, me)
			TKAD.xor_rank_pure(TKAD.view, me)
			--log:print("TKAD.select_peer", misc.time()-sp)
			return TKAD.view[math.random(TKAD.m)]
		else return TKAD.view[math.random(1,#TKAD.view)] end
		--return PSS.pss_getPeer()
	end,

	-- creates TKAD message: rank TKAD view by the xor_diff from the partner,
	-- select the highest ranked nodes for the message
	create_message = function(partner)
		--log:print("Starting TKAD.create_message")
		local cm = misc.time()
		local buffer = misc.dup(TKAD.view)
		buffer[#buffer+1] = me
		TKAD.remove_dup(buffer)
		TKAD.remove_self(buffer, partner)
		TKAD.xor_rank_pure(buffer, partner)
		--TKAD.xor_rank(buffer, partner)
		if #buffer > TKAD.m then TKAD.keep_n(buffer,TKAD.m) end
		log:print("TKAD.create_message", misc.time()-cm)
		return buffer
	end,

	--merges TKAD view with the received message
	update_view = function(received)
		--log:print("Starting TKAD.update_view")	
		local uv = misc.time()
		TKAD.view_lock:lock()
		TKAD.view = misc.merge(TKAD.view, received)
		TKAD.remove_dup(TKAD.view)
		TKAD.xor_rank(TKAD.view,me)
		TKAD.keep_n(TKAD.view, TKAD.v)
		TKAD.view_lock:unlock()
		--log:print("TKAD.update_view", misc.time()-uv)
	end,

	-- add nodes from the received to the corresponding k-buckets
	update_prefix_table = function(received)
		--log:print("Starting TKAD.update_prefix_table")
		local upt = misc.time()
		TKAD.rt_lock:lock()
		for i,v in ipairs(received) do
			local buck = TKAD.bucket_num(v)
			if not TKAD.routing_table[buck] then
				TKAD.routing_table[buck] = {}
				table.insert(TKAD.routing_table[buck], v)
			else
				if #TKAD.routing_table[buck] < TKAD.k then --bucket is not full
				--check if this element is already in the bucket: yes - move it to the tail, no - add it to the tail
					local index = TKAD.already_in(TKAD.routing_table[buck], v)
					if index > 0 then 
					table.insert(TKAD.routing_table[buck], table.remove(TKAD.routing_table[buck], index))
					else table.insert(TKAD.routing_table[buck], v) end			
				else --bucket full: ping the element at the head, if there is response - move to the tail; no response - evict and push the new element to the tail
					if TKAD.latency(TKAD.routing_table[buck][1]) then 
						table.insert(TKAD.routing_table[buck], table.remove(TKAD.routing_table[buck], 1))
					else
						table.remove(TKAD.routing_table[buck], 1)
						table.insert(TKAD.routing_table[buck], v)
					end
				end
			end
		end
		TKAD.rt_lock:unlock()
		--log:print("TKAD.update_prefix_table", misc.time()-upt)
	end,

	passive_thread = function(received,sender)
		local buffer = TKAD.create_message(sender)
		TKAD.update_view(received)
		TKAD.update_prefix_table(received)
		return buffer
	end,

	active_thread = function()
		local partner = TKAD.select_peer()
		local buffer = TKAD.create_message(partner)
		local try = 0
		local ok, res = rpc.acall(partner.peer, {'TKAD.passive_thread', buffer, me})
		while not ok do
			try = try + 1
			if try <= 3 then
				log:print("TKAD active thread: no response from:"..partner.id.. ": "..tostring(res).." => try again")
				events.sleep(math.random(try * 30, try * 60))
				ok, res = rpc.acall(partner.peer, {'TKAD.passive_thread', buffer, me})
			else
				log:print("TKAD active thread: no response from:"..partner.id..": "..tostring(res).."  => end")
				log:print("TKAD active thread: removing non-responding node from VIEW")
				TKAD.remove_failed_node(partner)
				break
			end
		end
		if ok then
			local received = res[1]
			local loc_cycle = TKAD.cycle + 1
			TKAD.cycle = TKAD.cycle + 1
			TKAD.update_view(received)
			TKAD.update_prefix_table(received)
			--TKAD.debug(loc_cycle,true)
		end
	end,
	
	}


function resource_stats()
	log:print("MEMORY_USED_Kb ", gcinfo())
	
	local ts,tr = socket.stats()
	local tot_KB_sent=misc.bitcalc(ts).kilobytes
	local tot_KB_recv=misc.bitcalc(tr).kilobytes

	log:print("BANDWIDTH_TOTAL ",tot_KB_sent, tot_KB_recv)
	log:print("BANDWIDTH_RATE  ", (tot_KB_sent - bytesSent )/STATS_PERIOD, (tot_KB_recv - bytesReceived) /STATS_PERIOD)

	bytesSent = tot_KB_sent
	bytesReceived = tot_KB_recv
end

-------------------------------------------------------------------------------
-- Main loop
-------------------------------------------------------------------------------
max_time = 600

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
	
	PSS.pss_init()
	events.sleep(20)
	PSS_thread = events.periodic(PSS_SHUFFLE_PERIOD, PSS.pss_active_thread)
	
	TKAD.precompute_routing_table()

	log:print("VIEW CONSTRUCTION START TIME", misc.time())	
	TKAD.init()
	events.sleep(20)
	
	TKAD_thread = events.periodic(GOSSIP_TIME, TKAD.active_thread)
	
	events.periodic(STATS_PERIOD, TKAD.check_convergence)
	
end

events.thread(main)
events.loop()
