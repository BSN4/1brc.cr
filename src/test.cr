require "./1brc/output"
require "./1brc/macros"
require "./1brc/stat"

module OneBRCParallel
  include OneBRC
  extend self

  alias FixPointInt = Int16
  alias Metric = Stat(FixPointInt, Int64)
  alias Collector = Hash(Bytes, Metric)

  TEMP10  = FixPointInt.new(10)
  TEMP100 = FixPointInt.new(100)
  PARALLEL_MAX = (ENV["CRYSTAL_WORKERS"]? || 4).to_i
  ZERO_ORD = UInt8.new('0'.ord)
  BUF_DIV = (ENV["BUF_DIV_DENOM"]? || 8).to_i
  PART_MAX = Int32::MAX // BUF_DIV
  PART_XTRA = 32

  class FileProcessor
    @part_size : Int32
    @file_parts : Int32

    def initialize(@file_path : String)
      @file = File.new(@file_path, "r")
      @file_size = @file.size
      @page_size = LibC.sysconf(LibC::SC_PAGESIZE)
      @part_size = calculate_part_size
      @file_parts = calculate_file_parts
    end

    def process(coll_chan : Channel(Collector))
      PARALLEL_MAX.times do |p_ix|
        spawn do
          back_buf = Bytes.new(@part_size + PART_XTRA)
          aggr = Collector.new
          p_ix.step(to: @file_parts - 1, by: PARALLEL_MAX) do |ix|
            ofs = ix.to_i64 * @part_size
            size = if ofs + @part_size < @file_size
                     @part_size
                   else
                     @file_size - ofs
                   end

            buf_size = size < @part_size ? size : size + PART_XTRA
            buf = back_buf[0, buf_size]
            @file.read_at(offset: ofs, bytesize: buf_size) { |io| io.read_fully(buf) }
            DataProcessor.process(ix, ofs, size.to_i32, buf, aggr)
          end
          coll_chan.send aggr
        end
      end
    end

    private def calculate_part_size
      part_size = PART_MAX
      unless (rem = part_size % @page_size).zero?
        part_size += @page_size - rem
      end
      part_size
    end

    private def calculate_file_parts
      (@file_size // @part_size).to_i32 + (@file_size % @part_size).clamp(0, 1).to_i32
    end
  end

  class BufferReader
    include OneBRC
    def initialize(@ptr : UInt8*, @size : Int32)
    end

    def find_line_start(read_pos)
      until @ptr[read_pos].ascii_newline?
        read_pos &+= 1
      end
      read_pos &+ 1
    end

    def find_next_line(read_pos)
      until @ptr[read_pos].ascii_newline?
        read_pos &+= 1
      end
      read_pos &+ 1
    end

    def read_until_separator(read_pos)
      start = read_pos
      while @ptr[read_pos] != ';'.ord
        read_pos &+= 1
      end
      {Bytes.new(@ptr + start, read_pos - start), read_pos &+ 1}
    end

    def unsafe_byte_at(pos)
      @ptr[pos]
    end
  end

  alias ParseState = {BufferReader, Int32, FixPointInt, FixPointInt}

  module ParserChain
    include OneBRC
    extend self

    def init(reader : BufferReader, read_pos : Int32) : ParseState
      {reader, read_pos, FixPointInt.new(0), FixPointInt.new(1)}
    end

    def read_sign(state : ParseState) : ParseState
      reader, read_pos, value, sign = state
      if reader.unsafe_byte_at(read_pos) == '-'.ord
        read_pos &+= 1
        sign = FixPointInt.new(-1)
      end
      {reader, read_pos, value, sign}
    end

    def read_digit(state : ParseState) : ParseState
      reader, read_pos, value, sign = state
      d = reader.unsafe_byte_at(read_pos) &- ZERO_ORD
      __expect_digit(reader.unsafe_byte_at(read_pos))
      value = value * TEMP10 &+ d
      read_pos &+= 1
      {reader, read_pos, value, sign}
    end

    def skip_char(state : ParseState) : ParseState
      reader, read_pos, value, sign = state
      {reader, read_pos &+ 1, value, sign}
    end

    def maybe_decimal(state : ParseState) : ParseState
      reader, read_pos, value, sign = state
      if reader.unsafe_byte_at(read_pos) == '.'.ord
        state = skip_char(state)
        state = read_digit(state)
      else
        state = read_digit(state)
        state = skip_char(state)
        state = read_digit(state)
      end
      state
    end

    def finalize(state : ParseState) : {FixPointInt, Int32}
      reader, read_pos, value, sign = state
      {sign * value, read_pos}
    end
  end

  module FastParse
    extend self

    def parse_temperature(reader : BufferReader, read_pos : Int32) : Tuple(FixPointInt, Int32)
       ParserChain.init(reader, read_pos)
        .try { |state| ParserChain.read_sign(state) }
        .try { |state| ParserChain.read_digit(state) }
        .try { |state| ParserChain.maybe_decimal(state) }
        .try { |state| ParserChain.finalize(state) }
    end
  end

  module DataProcessor
    include OneBRC
    extend self

    def process(ix : Int32, ofs : Int64, size : Int32, buf : Bytes, aggr : Collector)
      reader = BufferReader.new(buf.to_unsafe, size)
      read_pos = ofs > 0 ? reader.find_line_start(0) : 0

      while read_pos < size
        name, read_pos = reader.read_until_separator(read_pos)
        numval, read_pos = FastParse.parse_temperature(reader, read_pos)
        read_pos = reader.find_next_line(read_pos)

        if (rec = aggr[name]?)
          rec.add(numval)
        else
          aggr[name.dup] = Metric.new(numval)
        end
      end
    rescue ex
      STDERR.puts "Error processing chunk #{ix}: #{ex.message}"
      STDERR.puts ex.backtrace.join("\n")
    end
  end

  def run(input_file : String, output_file : String? = nil)
    file_processor = FileProcessor.new(input_file)
    coll_chan = Channel(Collector).new

    file_processor.process(coll_chan)

    aggr = Collector.new
    PARALLEL_MAX.times do
      next_aggr = coll_chan.receive
      next_aggr.each do |name, in_rec|
        if (rec = aggr[name]?)
          rec.add(in_rec)
        else
          aggr[name] = in_rec
        end
      end
    end

    output = output_file ? File.new(output_file, "w") : STDOUT
    write_output(output, aggr)
    output.close unless output == STDOUT
  end
end

OneBRCParallel.run(ARGV[0], ARGV[1]?)