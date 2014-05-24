var sys = require('util'),
    _ = require('underscore'),
    EventEmitter = require('events').EventEmitter;

var LEFT_CURLY_BRACE = 0x7b, //{
    RIGHT_CURLY_BRACE = 0x7d, //}
    QUOTE = 0x22, //"
    COMMA = 0x2C, //,
    LEFT_SQUARE_BRACE = 0x5b, //[
    RIGHT_SQUARE_BRACE = 0x5d; //]

var Parser = function()  {
    var self = this;
    self._braces = 0;
    self._sqBraces = 0;
    self.MAX = 20248; //fixed limit, hope everything we need fits
    self._globalBuffer = new Buffer(self.MAX); 
    self._buffLength = 0;
    self._mode = null;
};

/* 

Renew Output format, based on query or non-query mode for streaming

1. When using a filter
{
  "data": {
    "app.product": [
      {
        "_id": "5112dc456788576f9c007ee0",
        "displayName": "APM Service Assurance",
        "relationships": [
          
        ]
      },
      {
        "_id": "5112dc456788576f9c007ee6",
        "displayName": "BladeLogic Service Automation",
        "relationships": [
          
        ]
      }
    ]
  },
  "recordCount": {
    "app.product": 1
  },
  "success": true
}

2. When no filter is specified
[
{"_id":"5112dc456788576f9c007ee0","displayName":"APM Service Assurance","relationships":[]},
{"_id":"5112dc456788576f9c007ee6","displayName":"BladeLogic Service Automation","relationships":[]}
]

*/

sys.inherits(Parser, EventEmitter);

Parser.prototype.write = function(buffer)  {
    if (_.isEmpty(buffer)) return;

    var self = this,
        ch = null,
        obj = null;

    for(var i = 0; i < buffer.length; i++)  {
        ch = buffer[i];
        self._globalBuffer[self._buffLength++] = ch;

        switch (ch) {
            case COMMA: 
                if(this._braces == 0) 
                    self._buffLength--; //skip the , between objects
                break;
            case RIGHT_SQUARE_BRACE:
                self._sqBraces--;
                if (self._sqBraces < 0) 
                    self.emit('end');
                break;
            case LEFT_SQUARE_BRACE:
                self._sqBraces++; 

                if (!self._mode) {
                    self._mode = 'array';
                    self._buffLength = 0;
                }

                // Start of the actual payload, reset to start
                if (self._mode == 'namedCollectionMode' && self._sqBraces == 1) {
                    self._buffLength = 0;
                    self._braces = 0;
                }
                break;
            case LEFT_CURLY_BRACE: 
                if (!self._mode) {
                    self._mode = 'namedCollectionMode';
                } else {
                    self._braces++;                    
                }
                break;
            case RIGHT_CURLY_BRACE:
                if(--self._braces === 0)  {
                    try {
                        var str = self._globalBuffer.toString('utf8', 0, self._buffLength);
                        try {
                            obj = JSON.parse(str);
                            self._buffLength = 0;
                        } catch (x) {
                            console.log('Streaming parse error', x, str);
                            return self.emit('error', new Error('Parse error - ' + x));
                        }
                        process.nextTick(function(obj) {
                            return function() {
                                self.emit('data', obj);
                            };
                        } (obj));
                    } catch (x) {
                        self.emit('error', x);
                    }
                }
                break;
        }
    }
};

Parser.prototype.close = function()  {
    this.emit('end');    
};
exports.JSONParser = Parser;