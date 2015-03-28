module psql.common;

import
	std.traits,
	std.typetuple;

// Datatypes

alias u16 = ushort;
alias i16 = short;
alias u32 = uint;
alias i32 = int;
alias u64 = ulong;
alias i64 = long;

enum u16size = cast(u32) u16.sizeof;
enum u32size = cast(u32) u32.sizeof;

// UDA stuff

alias getUDAs(T) = TypeTuple!(__traits(getAttributes, T));

template matches(alias TypeA, alias TypeB)
{
	static if (is(TypeA : TypeB!TA, TA...)) // TypeB is a template
	{
		enum matches = true;
	}
	else
	{
		enum matches = is(TypeA : TypeB);
	}
}

template choose(alias UDA, UDAs...)
{
	static if (UDAs.length == 0)
	{
		alias choose = TypeTuple!();
	}
	else
	{
		static if (matches!(UDAs[0], UDA))
		{
			enum choose = UDAs[0].init;
		}
		else static if (matches!(typeof(UDAs[0]), UDA))
		{
			enum choose = UDAs[0];
		}
		else
		{
			alias choose = choose!(UDA, UDAs[1 .. $]);
		}
	}
}

enum hasUDA(T, alias UDA) = is(typeof(choose!(UDA, getUDAs!T)));
enum hasMember(T, string memberName) = is(typeof(__traits(getMember, T, memberName)));

// meta programming

template getDataMembers(T)
{
	template isDataMember(X)
	{
		static if (isSomeFunction!X)
		{
			enum isDataMember = false;
		}
		else static if (is(X == class) || is(X == struct) || is(X == union))
		{
			enum isDataMember = false;
		}
		else static if (isPointer!X)
		{
			enum isDataMember = false;
		}
		else
		{
			enum isDataMember = true;
		}
	}

	alias allMem = TypeTuple!(__traits(allMembers, T));

	string[] filteredMembers()
	{
		string[] result;
		foreach (mem; allMem)
		{
			static if (isDataMember!(typeof(__traits(getMember, T, mem))))
			{
				result ~= mem;
			}
		}
		return result;
	}

	alias getDataMembers = typeTuple!(filteredMembers());
}

template typeTuple(alias T)
{
	import std.typetuple;
	static if (T.length == 0)
	{
		alias typeTuple = TypeTuple!();
	}
	else
	{
		alias typeTuple = TypeTuple!(T[0], typeTuple!(T[1 .. $]));
	}
}
