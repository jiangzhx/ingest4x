-- batch_urls.lua
local file
local urls = {}
local current_index = 1
local batch_size = 100  -- 每批读取的 URL 数量

function setup(thread)
    thread:set("id", id)
end

function init(args)
    -- 初始化时打开文件
    file = io.open("urls.txt", "r")
    if not file then
        error("Failed to open URL file.")
    end
    read_batch_urls()  -- 读取第一批 URL
end

function read_batch_urls()
    urls = {}  -- 重置 URL 列表
    current_index = 1  -- 重置当前索引
    for i = 1, batch_size do
        local line = file:read()
        if line then
            table.insert(urls, line)
        else
            return false  -- 没有更多的行可读，返回 false
        end
    end
    return true  -- 成功读取一批 URL，返回 true
end

function request()
    if current_index > #urls then
        if not read_batch_urls() then  -- 尝试读取下一批 URL，如果没有更多，结束测试
            return nil  -- 返回 nil 以通知 wrk 结束测试
        end
    end
    local url = urls[current_index]
    current_index = current_index + 1
    return wrk.format(nil, url)
end

function done()
    -- 完成测试后关闭文件
    if file then
        file:close()
    end
end
