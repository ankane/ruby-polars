module Polars
  class SQLContext
    # @private
    attr_accessor :_ctxt, :_eager_execution

    def initialize(frames = nil, eager_execution: false, **named_frames)
      self._ctxt = RbSQLContext.new
      self._eager_execution = eager_execution

      frames = (frames || {}).to_h

      if frames.any? || named_frames.any?
        register_many(frames, **named_frames)
      end
    end

    def execute(query, eager: nil)
      res = Utils.wrap_ldf(_ctxt.execute(query))
      eager || _eager_execution ? res.collect : res
    end

    def register(name, frame)
      if frame.is_a?(DataFrame)
        frame = frame.lazy
      end
      _ctxt.register(name.to_s, frame._ldf)
      self
    end

    def register_many(frames, **named_frames)
      frames = (frames || {}).to_h
      frames = frames.merge(named_frames)
      frames.each do |name, frame|
        register(name, frame)
      end
      self
    end
  end
end
